"""Генератор ClickHouse DDL на CrewAI. Начни чтение с main()."""
import json
import os
import re
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from typing import Literal

from crewai import Agent, Crew, LLM, Task
from crewai.tools import tool
from pydantic import BaseModel


def main():
    messages = read_sample()
    settings = read_settings()
    fields = profile(messages)
    analyst, engineer, reviewer = create_agents()

    plan = agree_on_schema(analyst, fields)
    scripts = build_and_review(engineer, reviewer, settings, fields, plan)
    save(scripts.as_files())


class Settings(BaseModel):
    database: str
    base: str
    brokers: str
    topic: str
    group: str
    ttl_days: int


class Column(BaseModel):
    path: list[str]
    name: str
    dtype: Literal["String", "Int64", "UInt64", "Float64", "Bool", "JSON"]

class Plan(BaseModel):
    columns: list[Column]
    rationale: str

class Scripts(BaseModel):
    kafka: str
    detail: str
    mv: str

    def as_files(self):
        return {
            "01_kafka.sql": self.kafka,
            "02_detail.sql": self.detail,
            "03_mv.sql": self.mv,
        }


class Review(BaseModel):
    approved: bool
    findings: list[str]


def create_agents():
    model = LLM(model=os.getenv("MODEL", "gpt-4o-mini"), temperature=0)

    def agent(role, goal, background):
        return Agent(role=role, goal=goal, backstory=background, llm=model,
                     allow_delegation=False, verbose=False)

    analyst = agent("Аналитик событий", "Предложить схему для аналитических запросов",
                    "Ты проектируешь хранение разнородных Kafka-событий.")
    engineer = agent("ClickHouse-инженер", "Подготовить точные DDL по согласованной схеме",
                     "Ты используешь инструмент сборки DDL под ClickHouse 25.6.")
    reviewer = agent("Ревьюер DDL", "Проверить семантику всех трёх объектов",
                     "Ты проверяешь Kafka, MergeTree, типы, алиасы, TTL и сохранность JSON.")

    return analyst, engineer, reviewer


def agree_on_schema(analyst, fields):
    preferences = input("Какие поля нужны в колонках, какие оставить в message? "
                        "(Enter — предложит аналитик): ").strip()
    previous = ""
    while True:
        design = Task(agent=analyst, output_pydantic=Plan,
                      description="""Изучи профиль {profile} и пожелания {preferences}.
Предложи аналитические колонки: точный путь как список ключей, имя и тип.
Руководствуйся suggested_type. Массивы и сложные объекты оставляй в message,
если пользователь не попросил иное. Для JSON используется Nullable(String) с JSON-текстом.
Невыбранные ключи верхнего уровня автоматически останутся в message. При извлечении
вложенной колонки всё родительское поле останется в message. Не выбирай одновременно
объект и его потомка. Имена уникальны, кроме зарезервированных message и ingested_at.
Предыдущий вариант и замечания пользователя: {previous}. Объясни решения на русском.""",
                      expected_output="Структурированный план и короткое обоснование.")
        result = Crew(agents=[analyst], tasks=[design]).kickoff(inputs=dict(
            profile=json.dumps(fields, ensure_ascii=False), preferences=preferences, previous=previous))
        plan = result.pydantic
        if plan is None:
            raise ValueError("Аналитик не вернул структурированный план.")
        problem = schema_problem(plan, fields)
        if problem:
            print(problem)
            if previous.endswith(problem):
                raise ValueError(problem)
            feedback = problem
        else:
            show_schema(plan, fields)
            feedback = input("Enter — согласовать. Для изменения опиши правки: ").strip()
            if not feedback:
                return plan
        previous = plan.model_dump_json() + "\n" + feedback


def schema_problem(plan, fields):
    known = {tuple(field["path"]) for field in fields}
    selected = set()
    names = {"message", "ingested_at"}
    for column in plan.columns:
        path = tuple(column.path)
        if path not in known:
            return f"Поля {path} нет в примерах."
        if path in selected:
            return f"Поле {path} выбрано дважды."
        if not column.name.strip() or column.name in names:
            return f"Нужно другое имя колонки для {path}."
        selected.add(path)
        names.add(column.name)
    for path in selected:
        for depth in range(1, len(path)):
            if path[:depth] in selected:
                return f"Выбраны и объект {path[:depth]}, и его поле {path}."
    return None


def show_schema(plan, fields):
    print("\n" + plan.rationale)
    for column in plan.columns:
        print(f"  {column.path} → {column.name} ({column.dtype})")
    selected = [column.path for column in plan.columns]
    remaining = [f["path"][0] for f in fields if len(f["path"]) == 1 and f["path"] not in selected]
    print("В message:", remaining, "+ будущие ключи")


def build_and_review(engineer, reviewer, settings, fields, plan):
    expected = render(settings, plan.columns)

    @tool("build_clickhouse_ddl")
    def build_ddl() -> str:
        """Собрать три SQL по текущей утверждённой пользователем схеме."""
        return expected.model_dump_json()

    build = Task(agent=engineer, tools=[build_ddl], output_pydantic=Scripts,
                 description="Вызови build_clickhouse_ddl. Верни три полученных SQL без изменений.",
                 expected_output="Поля kafka, detail, mv с SQL.")
    audit = Task(agent=reviewer, context=[build], output_pydantic=Review,
                 description="""Проверь результат инженера по контракту {contract}.
Проверь имена, Kafka settings, источник и назначение MV, совпадение типов и алиасов,
TTL от ingested_at, сохранение неизвестных ключей и родительских объектов вложенных
колонок в message. Для ClickHouse 25.6.4.12. Существенные ошибки перечисли по-русски.
Установи approved=true только если существенных ошибок не найдено.""",
                 expected_output="Решение и список конкретных ошибок.")
    contract = {"settings": settings.model_dump(), "schema": plan.model_dump(), "profile": fields}
    crew = Crew(agents=[engineer, reviewer], tasks=[build, audit])
    result = crew.kickoff(inputs={"contract": json.dumps(contract, ensure_ascii=False)})
    if build.output is None or build.output.pydantic != expected:
        raise ValueError("SQL инженера расходится с результатом сборки по согласованной схеме.")
    if result.pydantic is None or not result.pydantic.approved:
        issues = result.pydantic.findings if result.pydantic else ["Нет решения ревьюера"]
        raise ValueError("DDL не прошли ревью: " + " / ".join(issues))
    return expected


def read_settings():
    topic = ask("Kafka-топик")
    base = ask("Базовое имя объектов", topic)
    db = ask("Существующая база ClickHouse", "default")
    brokers = ask("Брокеры Kafka, host:port через запятую")
    group = ask("Consumer group", f"ch_{db}_{base}")
    while True:
        days = ask("TTL в днях от загрузки (0 — без TTL)", "0")
        if re.fullmatch(r"[0-9]{1,6}", days):
            break
        print("Введи целое число от 0 до 999999.")
    return Settings(topic=topic, base=base, database=db, brokers=brokers, group=group, ttl_days=int(days))


def ask(label, default=None):
    while True:
        value = input(label + (f" [{default}]" if default is not None else "") + ": ").strip()
        if value or default is not None:
            return value or default



def read_sample():
    while True:
        try:
            path = input("Путь к JSON/JSONL (Enter — вставить JSON): ").strip().strip('"')
            if path:
                data = Path(path).read_text(encoding="utf-8-sig")
            else:
                print("Вставь JSON. Пустая строка завершает ввод.")
                data = "\n".join(iter(lambda: input(), ""))
            try:
                rows = json.loads(data)
                if not isinstance(rows, list):
                    rows = [rows]
            except json.JSONDecodeError:
                rows = [json.loads(line) for line in data.splitlines() if line.strip()]
            if rows and all(isinstance(row, dict) for row in rows):
                return rows
            print("Нужен JSON-объект, массив примеров или JSONL.")
        except (OSError, ValueError) as exc:
            print(exc)


def infer(values):
    values = [v for v in values if v is not None]
    kinds = {type(v) for v in values}
    if kinds == {str}:
        return "String"
    if kinds == {bool}:
        return "Bool"
    if kinds == {int}:
        if all(-(2**63) <= v < 2**63 for v in values):
            return "Int64"
        if all(0 <= v < 2**64 for v in values):
            return "UInt64"
    if float in kinds and kinds <= {int, float}:
        return "Float64"
    return "JSON"



def profile(rows, prefix=()):
    """Для каждого пути собираем типы. Значения из сообщений остаются локально."""
    result = []
    keys = dict.fromkeys(key for row in rows for key in row)
    for key in keys:
        path = [*prefix, key]
        values = [row.get(key) for row in rows]
        result.append({
            "path": path,
            "suggested_type": infer(values),
            "non_null": sum(value is not None for value in values),
            "observed_types": sorted({type(value).__name__ for value in values}),
        })
        children = [value for value in values if isinstance(value, dict)]
        if children:
            result.extend(profile(children, path))
    return result


def literal(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r").replace("\0", "\\0") + "'"


def ident(s):
    return "`" + s.replace("\\", "\\\\").replace("`", "\\`") + "`"



def render(settings, columns):
    def table(name):
        return ident(settings.database) + "." + ident(name)

    source = table(settings.base)
    target = table(settings.base + "_detail")
    view = table("mv_" + settings.base)
    ddl_source = f"""CREATE TABLE {source}
(
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = {literal(settings.brokers)},
    kafka_topic_list = {literal(settings.topic)},
    kafka_group_name = {literal(settings.group)},
    kafka_format = 'JSONAsString';"""
    defs = ["ingested_at DateTime('UTC')"]
    fields = ["now('UTC') AS ingested_at"]
    excluded = []
    for column in columns:
        path = column.path
        name = column.name
        dtype = column.dtype
        # При извлечении вложенного поля весь родитель остаётся в message.
        if len(path) == 1:
            excluded.append(path[0])
        keys = ", ".join(map(literal, path))
        if dtype == "JSON":
            typ = "Nullable(String)"
            expr = f"nullIf(JSONExtractRaw(src.payload, {keys}), '')"
        else:
            typ = f"Nullable({dtype})"
            expr = f"JSONExtract(src.payload, {keys}, {literal(typ)})"
        defs.append(f"{ident(name)} {typ}")
        fields.append(f"{expr} AS {ident(name)}")
    defs.append("message String")
    remainder = "src.payload"
    if excluded:
        keys = ", ".join(map(literal, excluded))
        remainder = f"""concat('{{', arrayStringConcat(arrayMap(
        p -> concat(toJSONString(p.1), ':', p.2),
        arrayFilter(p -> NOT has([{keys}], p.1), JSONExtractKeysAndValuesRaw(src.payload))
    ), ','), '}}')"""
    fields.append(remainder + " AS message")
    sep = ",\n    "
    expiry = f"\nTTL ingested_at + INTERVAL {settings.ttl_days} DAY DELETE" if settings.ttl_days else ""
    ddl_target = f"""CREATE TABLE {target}
(
    {sep.join(defs)}
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY ingested_at{expiry};"""
    ddl_mv = f"""CREATE MATERIALIZED VIEW {view} TO {target} AS
SELECT
    {sep.join(fields)}
FROM {source} AS src;"""
    return Scripts(kafka=ddl_source, detail=ddl_target, mv=ddl_mv)


def save(files):
    root = Path.cwd() / "ddl"
    root.mkdir(exist_ok=True)
    dest = Path(mkdtemp(prefix="kafka_", dir=root))
    try:
        for name, sql in files.items():
            (dest / name).write_text(sql + "\n", encoding="utf-8")
    except BaseException:
        rmtree(dest)
        raise
    print("Готово. SQL в порядке применения:", dest)
    for name in files:
        print(" ", name)
    return str(dest)



if __name__ == "__main__":
    main()

