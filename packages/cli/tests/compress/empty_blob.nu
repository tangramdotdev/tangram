use ../../test.nu *

# An empty blob compresses and decompresses back to itself.

let server = server spawn

let blob = "" | tg write --no-tokens

let compressed = tg compress --no-tokens --format gz $blob | str trim
assert ($compressed != $blob) "the compressed blob should differ from the empty blob"

let decompressed = tg decompress --no-tokens $compressed | str trim
assert equal $decompressed $blob "the decompressed blob should equal the empty blob"
