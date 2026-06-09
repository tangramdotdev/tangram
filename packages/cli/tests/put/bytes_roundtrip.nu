use ../../test.nu *

# Putting an object's raw bytes with only the kind flag recomputes the identical content-addressed id.

let server = spawn

let original = tg put 'tg.file("roundtrip")' | str trim
let bytes = tg get $original --bytes

let recomputed = $bytes | tg put --bytes --kind fil | str trim
assert ($recomputed == $original) "the recomputed id should match the original"
