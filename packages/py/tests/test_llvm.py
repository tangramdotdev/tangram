"""Tests for loading the Rust toolchain's shared LLVM library."""

import runpy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

Compiler = runpy.run_path(str(Path(__file__).resolve().parents[1] / "llvm.py"))[
    "Compiler"
]


class LlvmTests(unittest.TestCase):
    def test_skips_linker_scripts_and_identifies_the_loaded_library(self):
        with tempfile.TemporaryDirectory() as directory:
            sysroot = Path(directory)
            library_directory = sysroot / "lib"
            library_directory.mkdir()
            script = library_directory / "libLLVM-23-rust.so"
            script.write_text("INPUT(libLLVM.so.23)")
            library = library_directory / "libLLVM.so.23"
            library.write_bytes(b"\x7fELF")
            loaded = MagicMock()
            with patch(
                "ctypes.CDLL", side_effect=[OSError("file too short"), loaded]
            ) as load:
                compiler = Compiler(sysroot, "x86_64-unknown-linux-gnu")
            self.assertEqual(load.call_args_list[0].args, (str(script),))
            self.assertEqual(load.call_args_list[1].args, (str(library),))
            self.assertIs(compiler.library, loaded)
            self.assertEqual(
                compiler.identity,
                str(library.resolve()).encode()
                + str(library.stat().st_mtime_ns).encode(),
            )
            loaded.LLVMInitializeX86Target.assert_called_once_with()

    def test_preserves_the_loading_error_when_no_candidate_loads(self):
        with tempfile.TemporaryDirectory() as directory:
            sysroot = Path(directory)
            (sysroot / "lib").mkdir()
            (sysroot / "lib/libLLVM.so").touch()
            error = OSError("file too short")
            with (
                patch("ctypes.CDLL", side_effect=error),
                self.assertRaisesRegex(RuntimeError, "failed to load") as raised,
            ):
                Compiler(sysroot, "x86_64-unknown-linux-gnu")
            self.assertIs(raised.exception.__cause__, error)
