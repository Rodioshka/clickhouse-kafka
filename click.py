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
