import os
import tempfile
import unittest
from pathlib import Path

from scripts.benchmark.hits_vortex_standalone_bench import (
    discover_query_files,
    discover_query_dir,
    fingerprint_sql_for_query,
    load_sql_file,
    query_short_name,
    substitute_hits_table,
)


class TestHitsVortexStandaloneBench(unittest.TestCase):
    def test_query_short_name(self):
        self.assertEqual(query_short_name("00.sql"), "Q00")
        self.assertEqual(query_short_name("23.sql"), "Q23")

    def test_substitute_hits_table_preserves_other_identifiers(self):
        sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%hits%';"
        out = substitute_hits_table(sql, "hits_vortex")
        self.assertIn("FROM hits_vortex", out)
        self.assertNotIn("FROM hits ", out)

    def test_fingerprint_sql_wraps_query(self):
        sql = "SELECT COUNT(*) FROM hits;"
        fp = fingerprint_sql_for_query(sql, "hits_fuse")
        self.assertIn("WITH q AS", fp)
        self.assertIn("FROM q", fp)

    def test_discover_query_files_filters_sql(self):
        files = discover_query_files(["/tmp/00.sql", "/tmp/README.md", "/tmp/23.sql"])
        self.assertEqual(files, ["/tmp/00.sql", "/tmp/23.sql"])

    def test_discover_query_dir(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p / "00.sql").write_text("SELECT 1;", encoding="utf-8")
            (p / "23.sql").write_text("SELECT 2;", encoding="utf-8")
            (p / "note.txt").write_text("x", encoding="utf-8")
            files = discover_query_dir(str(p))
            self.assertEqual([os.path.basename(x) for x in files], ["00.sql", "23.sql"])


if __name__ == "__main__":
    unittest.main()

