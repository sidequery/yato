import os
import tempfile
import textwrap
import unittest

from yato import Yato
from yato.storage import Storage


class TestYato(unittest.TestCase):
    def test_yato_storage(self):
        yato = Yato(
            database_path="path",
            sql_folder="folder",
            s3_bucket="mybucket",
            s3_access_key="access",
            s3_secret_key="secret",
        )
        self.assertIsInstance(yato.storage, Storage)

    def test_yato_storage_none(self):
        yato = Yato(
            database_path="path",
            sql_folder="folder",
        )
        self.assertIsNone(yato.storage)

    def test_incremental_merge_with_lookback(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_dir = os.path.join(tmp_dir, "sql")
            os.makedirs(sql_dir, exist_ok=True)

            raw_source_path = os.path.join(sql_dir, "raw_source.sql")
            incremental_path = os.path.join(sql_dir, "orders.sql")

            with open(raw_source_path, "w", encoding="utf-8") as raw_file:
                raw_file.write(
                    textwrap.dedent(
                        """
                        SELECT *
                        FROM (
                            VALUES
                                (1, TIMESTAMP '2023-01-01 00:00:00', 'first'),
                                (2, TIMESTAMP '2023-01-02 00:00:00', 'second')
                        ) AS t(id, updated_at, payload)
                        """
                    ).strip()
                )

            with open(incremental_path, "w", encoding="utf-8") as incremental_file:
                incremental_file.write(
                    textwrap.dedent(
                        """
                        SELECT
                            id -- primary_key
                          , updated_at -- incremental_key, lookback: 1 day
                          , payload
                        FROM raw_source
                        """
                    ).strip()
                )

            database_path = os.path.join(tmp_dir, "db.duckdb")
            yato = Yato(
                database_path=database_path,
                sql_folder=sql_dir,
            )

            con, _ = yato.run()
            rows = con.sql(
                "SELECT id, payload FROM main.orders ORDER BY id"
            ).fetchall()
            self.assertEqual(rows, [(1, "first"), (2, "second")])

            with open(raw_source_path, "w", encoding="utf-8") as raw_file:
                raw_file.write(
                    textwrap.dedent(
                        """
                        SELECT *
                        FROM (
                            VALUES
                                (1, TIMESTAMP '2023-01-01 00:00:00', 'first'),
                                (2, TIMESTAMP '2023-01-02 00:00:00', 'second_updated'),
                                (3, TIMESTAMP '2023-01-03 00:00:00', 'third')
                        ) AS t(id, updated_at, payload)
                        """
                    ).strip()
                )

            con.close()

            con, _ = yato.run()
            rows = con.sql(
                "SELECT id, payload FROM main.orders ORDER BY id"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (1, "first"),
                    (2, "second_updated"),
                    (3, "third"),
                ],
            )

    def test_schema_inferred_from_folder(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_dir = os.path.join(tmp_dir, "sql")
            analytics_dir = os.path.join(sql_dir, "analytics")
            os.makedirs(analytics_dir, exist_ok=True)

            customers_path = os.path.join(sql_dir, "customers.sql")
            with open(customers_path, "w", encoding="utf-8") as customers_file:
                customers_file.write(
                    textwrap.dedent(
                        """
                        SELECT *
                        FROM (
                            VALUES (1, 'Alice'), (2, 'Bob')
                        ) AS t(id, name)
                        """
                    ).strip()
                )

            orders_path = os.path.join(analytics_dir, "orders.sql")
            with open(orders_path, "w", encoding="utf-8") as orders_file:
                orders_file.write(
                    textwrap.dedent(
                        """
                        SELECT id, name
                        FROM main.customers
                        """
                    ).strip()
                )

            database_path = os.path.join(tmp_dir, "db.duckdb")
            yato = Yato(
                database_path=database_path,
                sql_folder=sql_dir,
            )

            con, _ = yato.run()
            try:
                customers = con.sql("SELECT COUNT(*) FROM main.customers").fetchone()[0]
                orders = con.sql(
                    "SELECT name FROM analytics.orders ORDER BY id"
                ).fetchall()

                self.assertEqual(customers, 2)
                self.assertEqual(orders, [("Alice",), ("Bob",)])
            finally:
                con.close()

    def test_namespace_inference_can_be_disabled(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_dir = os.path.join(tmp_dir, "sql")
            analytics_dir = os.path.join(sql_dir, "analytics")
            os.makedirs(analytics_dir, exist_ok=True)

            customers_path = os.path.join(sql_dir, "customers.sql")
            with open(customers_path, "w", encoding="utf-8") as customers_file:
                customers_file.write(
                    textwrap.dedent(
                        """
                        SELECT *
                        FROM (
                            VALUES (1, 'Alice'), (2, 'Bob')
                        ) AS t(id, name)
                        """
                    ).strip()
                )

            orders_path = os.path.join(analytics_dir, "orders.sql")
            with open(orders_path, "w", encoding="utf-8") as orders_file:
                orders_file.write(
                    textwrap.dedent(
                        """
                        SELECT id, name
                        FROM customers
                        """
                    ).strip()
                )

            database_path = os.path.join(tmp_dir, "db.duckdb")
            yato = Yato(
                database_path=database_path,
                sql_folder=sql_dir,
                infer_namespaces=False,
            )

            con, _ = yato.run()
            try:
                customers = con.sql("SELECT COUNT(*) FROM main.customers").fetchone()[0]
                orders = con.sql(
                    "SELECT name FROM main.orders ORDER BY id"
                ).fetchall()

                self.assertEqual(customers, 2)
                self.assertEqual(orders, [("Alice",), ("Bob",)])
            finally:
                con.close()
