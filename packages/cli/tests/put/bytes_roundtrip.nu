use ../lib/test.nu *

# Putting an object's raw bytes with only the kind flag recomputes the identical content-addressed id.

let local = server spawn

let original = tg put --no-tokens 'tg.file("roundtrip")' | referent node
let bytes = tg get $original --bytes

let recomputed = $bytes | tg put --no-tokens --bytes --kind fil | referent node
assert ($recomputed == $original) "the recomputed id should match the original"
