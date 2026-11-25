"""Интерактивный генератор DDL. Запуск: python clickhouse_ddl_flow.py"""
import json
import re
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree


def ask(label, default=None):
    while True:
        value = input(label + (f" [{default}]" if default is not None else "") + ": ").strip()
        if value or default is not None:
            return value or default


def choice(label, options, default):
    while True:
        value = ask(label, default)
        if value in options:
            return value
        print("Варианты:", ", ".join(options))


def literal(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r").replace("\0", "\\0") + "'"


def ident(s):
    return "`" + s.replace("\\", "\\\\").replace("`", "\\`") + "`"


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


def render(db, base, brokers, topic, group, ttl, columns, excluded):
    source, target, view = [ident(db) + "." + ident(n) for n in
                            (base, base + "_detail", "mv_" + base)]
    ddl_source = f"""CREATE TABLE {source}
(
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = {literal(brokers)},
    kafka_topic_list = {literal(topic)},
    kafka_group_name = {literal(group)},
    kafka_format = 'JSONAsString';"""
    defs = ["ingested_at DateTime('UTC')"]
    fields = ["now('UTC') AS ingested_at"]
    for path, name, dtype in columns:
        keys = ", ".join(map(literal, path))
        typ = "Nullable(" + ("String" if dtype == "JSON" else dtype) + ")"
        expr = (f"nullIf(JSONExtractRaw(src.payload, {keys}), '')" if dtype == "JSON"
                else f"JSONExtract(src.payload, {keys}, {literal(typ)})")
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
    expiry = f"\nTTL ingested_at + INTERVAL {ttl} DAY DELETE" if ttl else ""
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
    return {"01_kafka.sql": ddl_source, "02_detail.sql": ddl_target, "03_mv.sql": ddl_mv}


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



def profile(rows):
    result = []
    def visit(objects, prefix):
        for key in dict.fromkeys(k for obj in objects for k in obj):
            values = [obj.get(key) for obj in objects]
            path = [*prefix, key]
            result.append(dict(path=path, suggested_type=infer(values),
                               non_null=sum(v is not None for v in values),
                               observed_types=sorted({type(v).__name__ for v in values})))
            children = [v for v in values if isinstance(v, dict)]
            if children:
                visit(children, path)
    visit(rows, [])
    return result


def main():
    import os
    from typing import Literal
    from pydantic import BaseModel
    from crewai import Agent, Crew, LLM, Process, Task
    from crewai.tools import tool

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

    class Review(BaseModel):
        approved: bool
        findings: list[str]

    llm = LLM(model=os.getenv("MODEL", "gpt-4o-mini"), temperature=0)
    def agent(role, goal, background):
        return Agent(role=role, goal=goal, backstory=background, llm=llm,
                     allow_delegation=False, verbose=False)

    analyst = agent("Аналитик событий", "Предложить схему для аналитических запросов",
                    "Ты проектируешь хранение разнородных Kafka-событий.")
    engineer = agent("ClickHouse-инженер", "Подготовить точные DDL по согласованной схеме",
                     "Ты используешь инструмент сборки DDL под ClickHouse 25.6.")
    reviewer = agent("Ревьюер DDL", "Проверить семантику всех трёх объектов",
                     "Ты проверяешь Kafka, MergeTree, типы, алиасы, TTL и сохранность JSON.")

    rows = read_sample()
    paths = profile(rows)
    known = {tuple(p["path"]) for p in paths}
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
            profile=json.dumps(paths, ensure_ascii=False), preferences=preferences, previous=previous))
        plan = result.pydantic
        if plan is None:
            raise ValueError("Аналитик не вернул структурированный план.")
        selected = [tuple(col.path) for col in plan.columns]
        names = [col.name for col in plan.columns]
        valid = (set(selected) <= known and len(set(selected)) == len(selected)
                 and all(not any(p[:i] in selected for i in range(1, len(p))) for p in selected)
                 and all(n.strip() for n in names)
                 and len(set(names) | {"message", "ingested_at"}) == len(names) + 2)
        print("\n" + plan.rationale)
        for col in plan.columns:
            print(f"  {json.dumps(col.path, ensure_ascii=False)} → {col.name} ({col.dtype})")
        retained = [p["path"][0] for p in paths if len(p["path"]) == 1 and tuple(p["path"]) not in selected]
        print("В message:", json.dumps(retained, ensure_ascii=False), "+ будущие ключи")
        if valid:
            feedback = input("Enter — согласовать. Для изменения опиши правки: ").strip()
            if not feedback:
                break
        else:
            feedback = "Исправь неизвестные или пересекающиеся пути, дубли и зарезервированные имена."
            print(feedback)
            if previous.endswith(feedback):
                raise ValueError("Аналитик повторно вернул несовместимый план.")
        previous = plan.model_dump_json() + "\n" + feedback

    cols = [(c.path, c.name, c.dtype) for c in plan.columns]
    files = render(db, base, brokers, topic, group, int(days), cols,
                   [p[0] for p, _, _ in cols if len(p) == 1])
    expected = Scripts(**dict(zip(("kafka", "detail", "mv"), files.values())))

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
    contract = dict(topic=topic, base=base, database=db, brokers=brokers, group=group,
                    ttl_days=int(days), schema=plan.model_dump(), profile=paths)
    result = Crew(agents=[engineer, reviewer], tasks=[build, audit],
                  process=Process.sequential).kickoff(inputs=dict(
                      contract=json.dumps(contract, ensure_ascii=False)))
    if build.output is None or build.output.pydantic != expected:
        raise ValueError("SQL инженера расходится с результатом сборки по согласованной схеме.")
    if result.pydantic is None or not result.pydantic.approved:
        issues = result.pydantic.findings if result.pydantic else ["Нет решения ревьюера"]
        raise ValueError("DDL не прошли ревью: " + " / ".join(issues))
    save(files)


if __name__ == "__main__":
    main()
