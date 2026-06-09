use ../../test.nu *

# An unsupported algorithm is rejected by the command line parser.

let server = spawn

let blob = "hello, world!\n" | tg write

let output = tg checksum --algorithm crc32 $blob | complete
failure $output
assert ($output.stderr | str contains "invalid value 'crc32'") "the error should mention the invalid algorithm value"
