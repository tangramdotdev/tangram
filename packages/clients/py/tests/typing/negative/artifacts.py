from tangram.directory import directory
from tangram.file import file
from tangram.sandbox import Sandbox
from tangram.symlink import symlink

file().executable("yes")  # error: invalid-argument-type
file().module(42)  # error: invalid-argument-type
symlink().path(42)  # error: invalid-argument-type
Sandbox.create().cpu("one")  # error: invalid-argument-type
directory().entry("invalid", 42)  # error: invalid-argument-type
