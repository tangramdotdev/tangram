"""Convert Astral's LLVM bitcode with the LLVM bundled with the Rust toolchain."""

import ctypes
import hashlib
from pathlib import Path


class Compiler:
    def __init__(self, sysroot: Path, target: str):
        libraries = sorted((sysroot / "lib").glob("*LLVM*.dylib")) + sorted(
            (sysroot / "lib").glob("*LLVM*.so*")
        )
        if not libraries:
            raise RuntimeError(
                "the Rust toolchain does not contain a shared LLVM library"
            )
        library = libraries[0]
        self.identity = (
            str(library.resolve()).encode() + str(library.stat().st_mtime_ns).encode()
        )
        self.library = ctypes.CDLL(str(library))
        self.pointer = ctypes.c_void_p
        self.error_pointer = ctypes.POINTER(ctypes.c_char_p)
        backend = "AArch64" if target.startswith("aarch64") else "X86"
        for suffix in ["TargetInfo", "Target", "TargetMC", "AsmParser", "AsmPrinter"]:
            self.function(f"LLVMInitialize{backend}{suffix}", None, [])()

    def function(self, name, result, args):
        function = getattr(self.library, name)
        function.restype = result
        function.argtypes = args
        return function

    def compile(self, source: Path, output: Path):
        digest = hashlib.sha256(
            source.read_bytes() + Path(__file__).read_bytes() + self.identity
        ).hexdigest()
        stamp = output.with_suffix(".sha256")
        if output.is_file() and stamp.is_file() and stamp.read_text() == digest:
            return
        pointer = self.pointer
        string = ctypes.c_char_p
        integer = ctypes.c_int
        pointer_pointer = ctypes.POINTER(pointer)
        context = self.function("LLVMContextCreate", pointer, [])()
        buffer = pointer()
        module = pointer()
        machine = None
        error = string()
        try:
            status = self.function(
                "LLVMCreateMemoryBufferWithContentsOfFile",
                integer,
                [string, pointer_pointer, self.error_pointer],
            )(str(source).encode(), ctypes.byref(buffer), ctypes.byref(error))
            if status:
                raise RuntimeError(error.value)
            status = self.function(
                "LLVMParseBitcodeInContext2",
                integer,
                [pointer, pointer, pointer_pointer],
            )(context, buffer, ctypes.byref(module))
            if status:
                raise RuntimeError(f"failed to parse the LLVM bitcode: {source}")
            # Keep CPython's allocator separate from Tangram's mimalloc instance.
            for kind in ["Function", "Global"]:
                value = self.function(f"LLVMGetFirst{kind}", pointer, [pointer])(module)
                while value:
                    length = ctypes.c_size_t()
                    name = self.function(
                        "LLVMGetValueName2",
                        ctypes.c_void_p,
                        [pointer, ctypes.POINTER(ctypes.c_size_t)],
                    )(value, ctypes.byref(length))
                    name = ctypes.string_at(name, length.value)
                    if name.startswith((b"mi_", b"_mi_")):
                        name = b"tangram_python_" + name
                        self.function(
                            "LLVMSetValueName2",
                            None,
                            [pointer, string, ctypes.c_size_t],
                        )(value, name, len(name))
                    value = self.function(f"LLVMGetNext{kind}", pointer, [pointer])(
                        value
                    )
            triple = self.function("LLVMGetTarget", string, [pointer])(module)
            target = pointer()
            status = self.function(
                "LLVMGetTargetFromTriple",
                integer,
                [string, pointer_pointer, self.error_pointer],
            )(triple, ctypes.byref(target), ctypes.byref(error))
            if status:
                raise RuntimeError(error.value)
            # Emit position-independent objects for both CRT linking modes.
            machine = self.function(
                "LLVMCreateTargetMachine",
                pointer,
                [pointer, string, string, string, integer, integer, integer],
            )(target, triple, b"generic", b"", 2, 2, 0)
            if not machine:
                raise RuntimeError("failed to create the LLVM target machine")
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_suffix(".tmp")
            status = self.function(
                "LLVMTargetMachineEmitToFile",
                integer,
                [pointer, pointer, string, integer, self.error_pointer],
            )(machine, module, str(temporary).encode(), 1, ctypes.byref(error))
            if status:
                raise RuntimeError(error.value)
            temporary.replace(output)
            stamp.write_text(digest)
        finally:
            if machine:
                self.function("LLVMDisposeTargetMachine", None, [pointer])(machine)
            if module:
                self.function("LLVMDisposeModule", None, [pointer])(module)
            if buffer:
                self.function("LLVMDisposeMemoryBuffer", None, [pointer])(buffer)
            self.function("LLVMContextDispose", None, [pointer])(context)
