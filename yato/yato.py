import logging
import os
import re
import tempfile
from typing import Iterable, Optional
from graphlib import TopologicalSorter

import duckdb
from rich.console import Console

from yato.mermaid import generate_mermaid_diagram
from yato.parser import (
    get_dependencies,
    is_select_tree,
    parse_sql,
    read_and_get_python_instance,
    read_sql,
)
from yato.storage import Storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.CRITICAL)


class RunContext:
    def __init__(self, con, fail_silently=False):
        """
        This class is a wrapper around the DuckDB connection object.
        :param con: DuckDB connection object.
        :param fail_silently: If True, it will not raise an error if an environment variable is not set. Default is False.
        """
        self.con = con
        self.fail_silently = fail_silently
        self.console = Console()

    def replace_env_vars(self, sql) -> str:
        """
        Replace the environment variables in the SQL.
        :param sql: The SQL to replace the environment variables in.
        :return: SQL with the environment variables replaced.
        """
        pattern = re.compile(r"\{\{\s*(\w+)\s*\}\}")

        def replace_match(match):
            var_name = match.group(1)
            if not self.fail_silently and os.getenv(var_name) is None:
                raise ValueError(f"Environment variable {var_name} is not set.")
            return os.getenv(var_name)

        return pattern.sub(replace_match, sql)

    def sql(self, sql):
        self.con.sql(self.replace_env_vars(sql))


class Yato:
    def __init__(
        self,
        database_path: str,
        sql_folder: str,
        dialect: str = "duckdb",
        schema: str = "transform",
        db_folder_name: str = "db",
        s3_bucket: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
        s3_endpoint_url: str = None,
        s3_region_name: str = None,
    ) -> None:
        """
        yato stands for yet another transformation orchestrator.

        The goal of yato is to provide the lightest SQL orchestrator on earth on top of DuckDB.
        You just need a bunch of SQL files in a folder, a configuration file, and you're good to go.
        Yato uses S3 compatible storage to backup and restore the DuckDB database between runs.

        :param database_path:  The path to the database file (reminder: only DuckDB is supported).
        :param sql_folder: The folder containing the SQL files to run.
        :param dialect: The SQL dialect to use in SQLGlot. Default is DuckDB and only DuckDB is supported.
        :param schema: The schema to use in the DuckDB database.
        :param db_folder_name: The name of the folder to use for backup and restore.
        :param s3_bucket: The S3 bucket to use for backup and restore.
        :param s3_access_key: The S3 access key.
        :param s3_secret_key: The S3 secret key.
        :param s3_endpoint_url: The S3 endpoint URL.
        :param s3_region_name: The region name.
        """
        self.database_path = database_path
        self.sql_folder = sql_folder
        self.dialect = dialect
        self.schema = schema
        self.db_folder_name = db_folder_name
        self.s3_bucket = s3_bucket
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_region_name = s3_region_name

    @property
    def storage(self) -> object or None:
        """
        Returns a boto3 S3 client if the S3 credentials are provided. Otherwise, returns None.
        :return: object or None
        """
        if self.s3_access_key and self.s3_secret_key and self.s3_bucket:
            return Storage(
                s3_access_key=self.s3_access_key,
                s3_secret_key=self.s3_secret_key,
                s3_endpoint_url=self.s3_endpoint_url,
                s3_region_name=self.s3_region_name,
            )
        return None

    def restore(self, overwrite=False) -> None:
        """
        Restores the DuckDB database from the S3 bucket.
        :param overwrite: If True, it will overwrite the existing database. Default is False.
        """
        logger.info(f"Restoring the DuckDB database from {self.s3_bucket}/{self.db_folder_name}...")
        with tempfile.TemporaryDirectory() as tmp_dirname:
            local_db_path = os.path.join(tmp_dirname, self.db_folder_name)

            os.mkdir(local_db_path)
            self.storage.download_folder(self.s3_bucket, self.db_folder_name, tmp_dirname)

            if overwrite and os.path.exists(self.database_path):
                logger.info(f"Overwrite activated. Removed {self.database_path}.")
                os.remove(self.database_path)

            con = duckdb.connect(self.database_path)
            con.sql(f"IMPORT DATABASE '{local_db_path}'")
        logger.info("Done.")

    def backup(self) -> None:
        """
        Backups the DuckDB database to the S3 bucket.
        """
        logger.info(f"Backing up the DuckDB database to {self.s3_bucket}/{self.db_folder_name}...")
        with tempfile.TemporaryDirectory() as tmp_dirname:
            local_db_path = os.path.join(tmp_dirname, self.db_folder_name)

            con = duckdb.connect(self.database_path)
            con.sql(f"EXPORT DATABASE '{local_db_path}' (FORMAT 'parquet')")
            self.storage.upload_folder(self.s3_bucket, local_db_path, self.db_folder_name)
        logger.info("Done.")

    def get_execution_order(self, dependencies):
        ts = TopologicalSorter({d: dependencies[d].deps for d in dependencies})
        return list(ts.static_order())

    def run_pre_queries(self, context: RunContext) -> None:
        context.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        context.sql(f"USE {self.schema}")

    def run_objects(self, execution_order, dependencies, context: RunContext) -> None:
        context.console.print(f"Running {len(execution_order)} objects...")
        with context.console.status("[bold green]Running...", speed=0.6) as status:
            for object_name in execution_order:
                if object_name not in dependencies:
                    context.console.print(f"[green]•[/] Identified {object_name} as a source.")
                    continue

                filename = dependencies[object_name].filename
                if os.path.exists(filename) and filename.endswith(".sql"):
                    status.update(f"[bold green]Running SQL {object_name}...")
                    try:
                        self.run_sql_query(filename, object_name, context)
                        context.console.print(f"[green]•[/] {object_name} completed.")
                    except Exception as e:
                        context.console.print(f"[red]Error running {object_name}: {e}[/]")
                elif os.path.exists(filename) and filename.endswith(".py"):
                    status.update(f"[bold green]Running Python {object_name}...")
                    self.run_python_query(filename, object_name, context)
                    context.console.print(f"[green]•[/] {object_name} completed.")
                else:
                    context.console.print(f"Identified {object_name} as a source.")

    def run_sql_query(self, filename, table_name, context: RunContext) -> None:
        """
        Runs a SQL query and creates a table in the DuckDB database.
        :param filename: Name of the SQL file to run.
        :param table_name: Name of the table to create.
        :param context: RunContext object.
        """
        sql = read_sql(filename)
        directives = self._extract_directives(sql)
        incremental_key = directives.get("incremental_key")
        primary_keys = directives.get("primary_key") or directives.get("unique_key")

        if incremental_key and primary_keys:
            if isinstance(primary_keys, str):
                primary_keys = [
                    key.strip() for key in primary_keys.split(",") if key.strip()
                ]
            lookback = directives.get("lookback")
            self._run_incremental_model(
                sql=sql,
                table_name=table_name,
                context=context,
                incremental_key=incremental_key,
                primary_keys=primary_keys,
                lookback=lookback,
            )
            return

        trees = parse_sql(sql, dialect=self.dialect)
        if len(trees) > 1:
            for tree in trees:
                if is_select_tree(tree):
                    context.sql(f"""CREATE OR REPLACE TABLE {self.schema}.{table_name} AS {tree}""")
                else:
                    context.sql(f"{tree}")
        else:
            context.sql(f"""CREATE OR REPLACE TABLE {self.schema}.{table_name} AS {sql}""")

    def run_python_query(self, filename, table_name, context: RunContext) -> None:
        """
        Runs a Python file and creates a table in the DuckDB database.
        :param filename: Name of the Python file to run.
        :param table_name: Name of the table to create.
        :param context: RunContext object.
        """
        instance = read_and_get_python_instance(filename)
        context.con.register("df", instance.run(context))
        context.sql(f"""CREATE OR REPLACE TABLE {self.schema}.{table_name} AS SELECT * FROM df""")

    def _extract_directives(self, sql: str) -> dict:
        directives: dict[str, object] = {}
        trees = parse_sql(sql, dialect=self.dialect)

        def record_list(key: str, values: Iterable[str]) -> None:
            existing = directives.get(key)
            if existing is None:
                existing_list: list[str] = []
            elif isinstance(existing, list):
                existing_list = existing
            elif isinstance(existing, str):
                existing_list = [existing]
            else:
                existing_list = []
            directives[key] = existing_list
            for value in values:
                if value and value not in existing_list:
                    existing_list.append(value)

        known_directives = {"incremental_key", "primary_key", "unique_key", "lookback"}

        def parse_comment(raw_comment: str, node_label: Optional[str] = None) -> None:
            cleaned = raw_comment.strip().lstrip("-").strip()
            if not cleaned:
                return

            fragments = [part.strip() for part in cleaned.split(",") if part.strip()]
            pending_key: Optional[str] = None
            pending_value = ""

            def finalize(key: str, value: str) -> None:
                directive = key.strip().lower()
                if directive not in known_directives:
                    return
                value = value.strip()
                if directive == "incremental_key":
                    target = value or node_label
                    if target:
                        directives[directive] = target
                elif directive in {"primary_key", "unique_key"}:
                    values = [v.strip() for v in value.split(",") if v.strip()] if value else []
                    if not values and node_label:
                        values = [node_label]
                    record_list(directive, values)
                elif directive == "lookback":
                    target = value or node_label
                    if target:
                        directives[directive] = target

            for fragment in fragments:
                if ":" in fragment:
                    key, value = fragment.split(":", 1)
                    if pending_key:
                        finalize(pending_key, pending_value)
                    pending_key = key
                    pending_value = value
                else:
                    candidate = fragment.strip()
                    lower_candidate = candidate.lower()
                    if lower_candidate in known_directives:
                        if pending_key:
                            finalize(pending_key, pending_value)
                        pending_key = candidate
                        pending_value = ""
                    elif pending_key:
                        addition = fragment.strip()
                        if addition:
                            pending_value = (
                                f"{pending_value}, {addition}" if pending_value else addition
                            )

            if pending_key:
                finalize(pending_key, pending_value)

        for tree in trees:
            for comment in getattr(tree, "comments", []) or []:
                parse_comment(comment)

            for node in tree.walk():
                comments = getattr(node, "comments", None)
                if not comments:
                    continue
                node_label = self._infer_expression_label(node)
                for comment in comments:
                    parse_comment(comment, node_label=node_label)

        return directives

    @staticmethod
    def _infer_expression_label(node) -> Optional[str]:
        alias = getattr(node, "alias", None)
        if alias:
            return alias
        alias_or_name = getattr(node, "alias_or_name", None)
        if alias_or_name:
            return alias_or_name
        name = getattr(node, "name", None)
        if name:
            return name
        base = getattr(node, "this", None)
        if base is not None and base is not node:
            nested = Yato._infer_expression_label(base)
            if nested:
                return nested
        return None

    def _table_exists(self, context: RunContext, table_name: str) -> bool:
        query = f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = '{self.schema}'
          AND table_name = '{table_name}'
        LIMIT 1
        """
        return context.con.sql(query).fetchone() is not None

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _prepare_lookback_expression(raw_value: Optional[str]) -> Optional[str]:
        if not raw_value:
            return None
        value = raw_value.strip()
        interval_match = re.match(r"^(\d+)\s*(\w+)$", value)
        if interval_match:
            amount, unit = interval_match.groups()
            return f"INTERVAL '{amount}' {unit.upper()}"
        return value

    def _run_incremental_model(
        self,
        sql: str,
        table_name: str,
        context: RunContext,
        incremental_key: str,
        primary_keys: list[str],
        lookback: Optional[str] = None,
    ) -> None:
        target_table = f"{self.schema}.{table_name}"
        base_query = sql.strip().rstrip(";")

        relation = context.con.sql(f"SELECT * FROM ({base_query}) AS __yato_source LIMIT 0")
        columns = relation.columns

        if not columns:
            raise ValueError(f"No columns returned for incremental model {table_name}.")

        column_lookup = {col.lower(): col for col in columns}

        incremental_column = column_lookup.get(incremental_key.strip().lower())
        if not incremental_column:
            raise ValueError(
                f"Incremental key '{incremental_key}' not found in the result set of {table_name}."
            )

        normalized_primary_keys = [key.strip() for key in primary_keys if key.strip()]
        if not normalized_primary_keys:
            raise ValueError(f"Primary key not provided for incremental model {table_name}.")

        resolved_primary_keys = []
        for key in normalized_primary_keys:
            resolved = column_lookup.get(key.lower())
            if not resolved:
                raise ValueError(
                    f"Primary key column '{key}' not found in the result set of {table_name}."
                )
            resolved_primary_keys.append(resolved)

        if not self._table_exists(context, table_name):
            context.sql(f"""CREATE OR REPLACE TABLE {target_table} AS {base_query}""")
            return

        quoted_incremental = self._quote_identifier(incremental_column)
        lookback_expr = self._prepare_lookback_expression(lookback)
        max_expression = f"MAX({quoted_incremental})"
        if lookback_expr:
            max_expression = f"{max_expression} - {lookback_expr}"

        quoted_primary_keys = [self._quote_identifier(col) for col in resolved_primary_keys]
        primary_key_set = {pk.lower() for pk in resolved_primary_keys}
        update_columns = [col for col in columns if col.lower() not in primary_key_set]
        update_clause = ""
        if update_columns:
            assignments = ", ".join(
                [
                    f"target.{self._quote_identifier(col)} = source.{self._quote_identifier(col)}"
                    for col in update_columns
                ]
            )
            update_clause = f"\nWHEN MATCHED THEN UPDATE SET {assignments}"

        insert_columns = ", ".join(self._quote_identifier(col) for col in columns)
        insert_values = ", ".join(f"source.{self._quote_identifier(col)}" for col in columns)
        filter_clause = (
            "\n            WHERE "
            + f"{quoted_incremental} >= COALESCE("
            + "\n                (SELECT "
            + f"{max_expression} FROM {target_table}),"
            + "\n                (SELECT MIN("
            + f"{quoted_incremental}) FROM source_data)"
            + "\n            )"
            + "\n        "
        )

        source_cte = f"""
        WITH source_data AS (
            {base_query}
        ),
        filtered AS (
            SELECT *
            FROM source_data
            {filter_clause}
        )
        """

        on_conditions = " AND ".join(
            [f"target.{col} = source.{col}" for col in quoted_primary_keys]
        )

        merge_sql = f"""
        {source_cte}
        MERGE INTO {target_table} AS target
        USING filtered AS source
        ON {on_conditions}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """

        try:
            context.sql(merge_sql)
        except duckdb.Error as exc:
            if "MERGE" not in str(exc).upper():
                raise
            self._run_incremental_without_merge(
                context=context,
                target_table=target_table,
                source_cte=source_cte,
                columns=columns,
                quoted_primary_keys=quoted_primary_keys,
            )

    def _run_incremental_without_merge(
        self,
        context: RunContext,
        target_table: str,
        source_cte: str,
        columns: list[str],
        quoted_primary_keys: list[str],
    ) -> None:
        temp_table = "__yato_incremental"
        select_columns = ", ".join(
            f"source.{self._quote_identifier(col)}" for col in columns
        )
        create_temp_sql = f"""
        CREATE OR REPLACE TEMP TABLE {temp_table} AS
        {source_cte}
        SELECT *
        FROM filtered
        """

        context.sql(create_temp_sql)

        try:
            delete_conditions = " AND ".join(
                [f"{col} = source.{col}" for col in quoted_primary_keys]
            )
            delete_sql = f"""
            DELETE FROM {target_table}
            WHERE EXISTS (
                SELECT 1
                FROM {temp_table} AS source
                WHERE {delete_conditions}
            )
            """

            context.sql(delete_sql)

            insert_columns = ", ".join(self._quote_identifier(col) for col in columns)
            insert_sql = f"""
            INSERT INTO {target_table} ({insert_columns})
            SELECT {select_columns}
            FROM {temp_table} AS source
            """

            context.sql(insert_sql)
        finally:
            context.sql(f"DROP TABLE IF EXISTS {temp_table}")

    def run(self) -> object:
        """
        Runs do all the magic, it parses all the SQL queries, resolves the dependencies,
        and runs the queries in the guessed order.

        :return: Then it returns a DuckDB connection object.
        """
        con = duckdb.connect(self.database_path)
        context = RunContext(con)
        dependencies = get_dependencies(self.sql_folder, self.dialect)
        execution_order = self.get_execution_order(dependencies)
        self.run_pre_queries(context)
        self.run_objects(execution_order, dependencies, context)
        generate_mermaid_diagram(self.sql_folder, dependencies)
        return con, context
