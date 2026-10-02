use ../lib/test.nu *

# The algorithm flag defaults to sha256.

let local = server spawn

let blob = "hello, world!\n" | tg write --no-tokens | referent node

let default_checksum = tg checksum $blob | from json
let sha256_checksum = tg checksum --algorithm sha256 $blob | from json
assert equal $default_checksum $sha256_checksum "the default algorithm should be sha256"
