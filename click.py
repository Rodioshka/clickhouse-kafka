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


def mappings(rows):
    columns, excluded = [], []
    taken = {"message", "ingested_at"}
    def visit(objects, prefix):
        for key in dict.fromkeys(k for obj in objects for k in obj):
            path = prefix + [key]
            values = [obj.get(key) for obj in objects]
            kind = infer(values)
            children = [v for v in values if isinstance(v, dict)]
            print("\nПоле", json.dumps(path, ensure_ascii=False), "• тип по примерам:", kind)
            options = ["c", "m"]
            label = "c — колонка, m — оставить в message"
            if children:
                options.append("n")
                label += ", n — выбрать вложенные колонки и сохранить объект в message"
            action = choice(label, options, "m" if kind == "JSON" else "c")
            if action == "n":
                visit(children, path)
            elif action == "c":
                hint = re.sub(r"[^\w]+", "_", "_".join(path)).strip("_") or "field"
                if hint in taken:
                    hint += "_value"
                while True:
                    name = ask("Имя колонки", hint)
                    if name not in taken:
                        break
