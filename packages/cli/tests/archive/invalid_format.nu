use ../../test.nu *

# Unsupported archive format and compression values are rejected by the command line parser.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let format_output = tg archive --format rar $dir | complete
failure $format_output
assert ($format_output.stderr | str contains "invalid value 'rar'") "the error should mention the invalid format value"

let compression_output = tg archive --format tar --compression lz4 $dir | complete
failure $compression_output
assert ($compression_output.stderr | str contains "invalid value 'lz4'") "the error should mention the invalid compression value"
