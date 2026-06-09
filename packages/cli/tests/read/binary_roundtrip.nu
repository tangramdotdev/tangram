use ../../test.nu *

# Writing and reading a blob preserves binary bytes exactly.

let server = spawn

let bytes = 0x[00 01 02 fe ff 7f 80]
let blob = $bytes | tg write | str trim

let contents = tg read $blob | into binary
assert equal $contents $bytes "the read bytes should match the written bytes"
