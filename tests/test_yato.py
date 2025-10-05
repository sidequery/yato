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
                "SELECT id, payload FROM transform.orders ORDER BY id"
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
                "SELECT id, payload FROM transform.orders ORDER BY id"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (1, "first"),
                    (2, "second_updated"),
                    (3, "third"),
                ],
            )
