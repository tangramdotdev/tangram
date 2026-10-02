"""JavaScript Printer goldens for structural and diagnostic rendering."""

import unittest

import tangram as tg
from tangram.mutation import UNSET
from tangram.value.print import Printer


class PrintTests(unittest.TestCase):
    def test_scalars_and_escaping(self):
        self.assertEqual(
            Printer().print(
                [UNSET, None, True, 1.0, -0.0, 1e-6, 1e21, b"\x00\xff", "\ud800"]
            ),
            '[undefined,null,true,1,0,0.000001,1e+21,tg.bytes("AP8="),"\\ud800"]',
        )
        self.assertEqual(Printer().print(float("nan")), "NaN")
        self.assertEqual(Printer().print(float("inf")), "Infinity")

    def test_indentation_and_colors(self):
        printer = Printer({"indentation": "  ", "indent": 1})
        self.assertEqual(
            printer.print({"a": [1, {}]}),
            '{\n    "a": [\n      1,\n      {},\n    ],\n  }',
        )
        self.assertEqual(printer.indent_, 1)
        self.assertEqual(
            Printer({"color": True}).print({"a": None}),
            '{\x1b[32m"a"\x1b[0m:\x1b[38;5;244mnull\x1b[0m}',
        )
        self.assertEqual(
            Printer().print({"a": 0, "10": 1, "2": 2, "01": 3, "4294967295": 4}),
            '{"2":2,"10":1,"a":0,"01":3,"4294967295":4}',
        )

    def test_blob_forms(self):
        printer = Printer()
        self.assertEqual(printer.print(tg.Blob("hello")), 'tg.blob("hello")')
        self.assertEqual(printer.print(tg.Blob(b"\xff")), "tg.blob()")
        self.assertEqual(
            printer.print(tg.Blob.with_object({"children": []})), "tg.blob({})"
        )
        self.assertEqual(
            printer.print(
                tg.Blob.with_object({"children": [{"length": 1, "blob": tg.Blob("a")}]})
            ),
            'tg.blob({"length":1,"blob":tg.blob("a")})',
        )

    def test_graph_edges_and_dependencies(self):
        graph = tg.Graph([{"kind": "directory", "entries": {"self": 0}}])
        pointer = graph.pointer(0, "directory")
        self.assertEqual(
            Printer().print(pointer.artifact()),
            'tg.directory({"graph":tg.graph({"nodes":[{"kind":"directory","entries":{"self":0}}]}),"index":0,"kind":"directory"})',
        )
        self.assertEqual(
            Printer().graph_dependency(
                {
                    "node": pointer,
                    "options": {"name": "dep", "tokens": {"local": ["secret"]}},
                }
            ),
            '{"node":{"graph":tg.graph({"nodes":[{"kind":"directory","entries":{"self":0}}]}),"index":0,"kind":"directory"},"options":{"name":"dep"}}',
        )

    def test_command_values(self):
        command = tg.Command(
            "sh",
            args=["-c", "echo hi"],
            env={"A": tg.Command.Value.value(1)},
            host="test",
            stdin=tg.Blob("input"),
        )
        self.assertEqual(
            Printer().print(command),
            'tg.command({"args":[{"kind":"string","value":"-c"},'
            '{"kind":"string","value":"echo hi"}],'
            '"env":{"A":{"kind":"value","value":1}},'
            '"executable":{"path":"sh"},"host":"test",'
            '"stdin":tg.blob("input")})',
        )

    def test_error_diagnostic_and_source(self):
        module = tg.Module(
            "ts",
            tg.Referent("./main.ts", {"name": "main", "tokens": {"local": ["secret"]}}),
        )
        range_ = {
            "start": {"line": 1, "character": 2},
            "end": {"line": 3, "character": 4},
        }
        location = {
            "file": {"kind": "module", "value": module},
            "range": range_,
            "symbol": "main",
        }
        error = tg.Error.with_object(
            {
                "diagnostics": [
                    {
                        "location": {"module": module, "range": range_},
                        "message": "bad",
                        "severity": "error",
                    }
                ],
                "location": location,
                "message": "outer",
                "source": tg.Referent({"message": "inner"}, {"name": "cause"}),
                "stack": [location],
                "values": {},
            }
        )
        module_text = (
            '{"kind":"ts","referent":{"node":"./main.ts","options":{"name":"main"}}}'
        )
        range_text = '{"start":{"line":1,"character":2},"end":{"line":3,"character":4}}'
        location_text = (
            '{"file":{"kind":"module","value":'
            + module_text
            + '},"range":'
            + range_text
            + ',"symbol":"main"}'
        )
        self.assertEqual(
            Printer().print(error),
            'tg.error({"diagnostics":[{"location":{"module":'
            + module_text
            + ',"range":'
            + range_text
            + '},"message":"bad","severity":"error"}],"location":'
            + location_text
            + ',"message":"outer","source":{"node":tg.error({"message":"inner"}),'
            '"options":{"name":"cause"}},"stack":[' + location_text + "]})",
        )
        self.assertEqual(
            Printer().error_source(
                tg.Referent(
                    tg.Error.with_object({"message": "inner"}),
                    {"tokens": {"local": ["secret"]}},
                )
            ),
            'tg.error({"message":"inner"})',
        )

    def test_mutation_and_template(self):
        mutation = tg.Mutation(
            {
                "kind": "suffix",
                "template": tg.Template(["tail"]),
                "separator": None,
                "extra": 1,
            }
        )
        self.assertEqual(
            Printer().print(mutation),
            'tg.mutation({"kind":"suffix","template":tg`tail`,"extra":1})',
        )
        self.assertEqual(
            Printer().print(tg.Template(["\\`${", tg.Placeholder("output")])),
            'tg`\\\\\\`\\${${tg.placeholder("output")}`',
        )

    def test_unloaded_id_and_invalid_value(self):
        blob = tg.Blob.with_id(
            "blb_01zby8hmr9wc7c8t2g8c7qt29cyt8hkeg2y6y1yahh585dx6hebf2g"
        )
        self.assertEqual(Printer().print(blob), blob.id)
        with self.assertRaisesRegex(TypeError, "invalid value"):
            Printer().print(object())
