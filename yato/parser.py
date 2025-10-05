import os
from dataclasses import dataclass
from typing import Optional

from sqlglot import exp, parse

from yato.python_context import load_class_from_file_path


def get_table_name(table) -> str:
    """
    Get the name of a SQLGlot Table object with the database and catalog if available.
    Also handles DuckDB table functions like read_csv and read_parquet.
    :param table: The SQLGlot Table object.
    :return: The name of the table as a string.
    """
    # import pdb; pdb.set_trace()
    if isinstance(table.this, exp.Anonymous):
        if table.this.this == "read_parquet":
            return table.this.expressions[0].this
    if isinstance(table.this, exp.ReadCSV):
        return table.this.this.this

    db = table.text("db")
    catalog = table.text("catalog")
    output = ""

    if db:
        output += db + "."
    if catalog:
        output += catalog + "."

    return output + table.name


def snake_to_camel(snake_str):
    """
    Convert a snake_case string to camelCase.
    :param snake_str: The snake_case string to convert.
    :return: The camelCase string.
    """
    components = snake_str.split("_")
    return "".join(x.title() for x in components)


def read_and_get_python_instance(filename):
    """
    Read the Python file and return the instance of the class with the same name as the file.
    :param filename: The name of the file to read.
    :return: The instance of the class with the same name as the file.
    """
    name, ext = os.path.splitext(filename)
    if ext == ".py":
        class_name = snake_to_camel(os.path.basename(name))
        klass = load_class_from_file_path(filename, class_name)
        instance = klass()
        return instance


def read_sql(filename) -> str:
    """
    Read the SQL from the given file.
    If the file is a Python file it will read the SQL query using the yato helpers.
    :param filename: The name of the file to read.
    :return: The content of the file as a string.
    """

    name, ext = os.path.splitext(filename)
    if ext == ".py":
        instance = read_and_get_python_instance(filename)
        return instance.source_sql()

    if ext == ".sql":
        with open(filename, "r") as f:
            sql = f.read()
        return sql


def is_select_tree(tree):
    """
    Check if the given SQLGlot tree is a SELECT query.
    :param tree: The SQLGlot tree to check.
    :return: True if the tree is a SELECT query, False otherwise.
    """
    return isinstance(tree, exp.Select) or isinstance(tree, exp.CTE) or isinstance(tree, exp.Pivot)


def is_insert_tree(tree):
    """
    Check if the given SQLGlot tree is an INSERT query.
    :param tree: The SQLGlot tree to check.
    :return: True if the tree is an INSERT query, False otherwise.
    """
    return isinstance(tree, exp.Insert)


def find_select_query(trees: list[exp.Expression]):
    """
    Find the select query in the given list of SQLGlot trees.
    :param trees: SQLGlot trees list.
    :return:
    """
    if sum([is_select_tree(t) for t in trees]) > 1:
        raise ValueError("Only one SELECT query is allowed.")
    if sum([is_insert_tree(t) for t in trees]) == 1:
        return [t for t in trees if is_insert_tree(t)][0]
    return [t for t in trees if is_select_tree(t)][0]


def parse_sql(sql, dialect="duckdb"):
    """
    Parse the given SQL using the SQLGlot parser.
    :param sql: The SQL to parse.
    :param dialect: The dialect to use for parsing the SQL.
    :return: The data returned is a list of SQLGlot trees.
    """
    return parse(sql, dialect=dialect)


def get_tables(sql, dialect="duckdb") -> list[str]:
    """
    Get the tables used in the given SQL.

    :param sql: The SQL to parse.
    :param dialect: The dialect to use for parsing the SQL.
    :return: The data returned is a list of table names used in the SQL.
    """
    trees = parse_sql(sql, dialect=dialect)
    select = find_select_query(trees)
    ctes = [c.alias_or_name for c in select.find_all(exp.CTE)]
    all_tables = [get_table_name(t) for t in select.find_all(exp.Table)]
    return list(set([t for t in all_tables if t not in ctes]))


@dataclass(frozen=True)
class Relation:
    schema: str
    name: str
    database: Optional[str] = None

    @property
    def canonical_name(self) -> str:
        parts = [self.schema, self.name]
        if self.database:
            parts.insert(0, self.database)
        return ".".join(parts)

    @property
    def schema_identifier(self) -> str:
        if self.database:
            return f"{self.database}.{self.schema}"
        return self.schema

    def qualify(self, table_name: str) -> str:
        parts = [table_name]
        if self.schema:
            parts.insert(0, self.schema)
        if self.database:
            parts.insert(0, self.database)
        return ".".join(parts)


@dataclass
class Dependency:
    deps: list[str]
    filename: str
    relation: Relation


def _relation_from_filename(
    folder: str,
    filename: str,
    default_schema: str,
    infer_namespaces: bool,
) -> Relation:
    relative = os.path.relpath(os.path.dirname(filename), folder)
    table_name = os.path.splitext(os.path.basename(filename))[0]

    if not infer_namespaces:
        return Relation(schema=default_schema, name=table_name)

    if relative in (".", ""):
        return Relation(schema=default_schema, name=table_name)

    parts = [part for part in relative.split(os.sep) if part]

    if len(parts) == 1:
        return Relation(schema=parts[0], name=table_name)
    if len(parts) == 2:
        return Relation(database=parts[0], schema=parts[1], name=table_name)
    if len(parts) > 2:
        raise ValueError(
            "SQL files can be nested in at most two directories (database/schema)."
        )

    return Relation(schema=default_schema, name=table_name)


def _canonicalize_dependency_names(raw_names: list[str], relation: Relation) -> list[str]:
    canonical: set[str] = set()
    for name in raw_names:
        if "." not in name:
            canonical.add(relation.qualify(name))
        else:
            canonical.add(name)
    return sorted(canonical)


def get_dependencies(
    folder,
    dialect: str = "duckdb",
    default_schema: str = "main",
    infer_namespaces: bool = True,
) -> dict:
    """
    Get the dependencies of the files (SQL or Python) in the given folder.

    :param folder: The folder in which the files (SQL or Python) are located.
    :param dialect: The dialect to use for parsing the SQL queries.
    :return: The data returned is a dictionary with the filenames as keys and the tables used in the
             SQL queries as values.
    """
    dependencies = {}
    effective_default_schema = default_schema or "main"

    for dirpath, dirnames, filenames in os.walk(folder):
        for file in filenames:
            name, ext = os.path.splitext(file)
            if ext == ".sql" or ext == ".py":
                filename = os.path.join(dirpath, file)
                relation = _relation_from_filename(
                    folder,
                    filename,
                    effective_default_schema,
                    infer_namespaces=infer_namespaces,
                )
                sql = read_sql(filename)
                tables = get_tables(sql, dialect)
                deps = _canonicalize_dependency_names(tables, relation)
                dependencies[relation.canonical_name] = Dependency(
                    deps=deps,
                    filename=filename,
                    relation=relation,
                )
    return dependencies
