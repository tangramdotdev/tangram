use ../../test.nu *

# Decompressing a blob that is not compressed fails with an invalid compression format error.

let server = server spawn

let blob = "hello, world!\n" | tg write

let output = tg decompress $blob | complete
failure $output
