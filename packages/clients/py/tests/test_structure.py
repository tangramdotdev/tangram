"""Keep the Python source layout aligned with the JavaScript client."""

import unittest
from pathlib import Path

import tangram as tg


class StructureTests(unittest.TestCase):
    def test_javascript_files_have_python_counterparts(self):
        clients = Path(__file__).resolve().parents[2]
        python = clients / "py/src/tangram"
        exceptions = {
            "assert.ts": "assert_.py",
            "host/node.ts": "host/default.py",
            "index.ts": "__init__.py",
        }
        for source in sorted((clients / "js/src").rglob("*.ts")):
            relative = source.relative_to(clients / "js/src")
            target = python / exceptions.get(
                str(relative), str(relative.with_suffix(".py"))
            )
            with self.subTest(source=str(relative)):
                self.assertTrue(
                    target.is_file()
                    or (target.with_suffix("") / "__init__.py").is_file(),
                    f"missing counterpart for {relative}",
                )

    def test_implementations_live_in_their_modules(self):
        self.assertEqual(tg.Template.__module__, "tangram.template")
        self.assertEqual(tg.Placeholder.__module__, "tangram.placeholder")
        self.assertEqual(tg.Command.Builder.__module__, "tangram.command")
        self.assertEqual(tg.Process.Builder.__module__, "tangram.process")
        self.assertEqual(tg.File.__module__, "tangram.file")
        self.assertEqual(tg.http.Body.__module__, "tangram.http.body")
        self.assertEqual(tg.http.Request.__module__, "tangram.http.request")
