use ../lib/test.nu *

# Decompressing a blob that is not compressed fails with an invalid compression format error.

let local = server spawn

let blob = "hello, world!\n" | tg write --no-tokens | referent node

let output = tg decompress $blob | complete
failure $output
