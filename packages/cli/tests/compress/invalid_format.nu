use ../../test.nu *

# Compressing with an unsupported format is rejected by the command line parser.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg compress --format lz4 $blob | complete
failure $output
assert ($output.stderr | str contains "invalid value 'lz4'") "the error should mention the invalid format value"
