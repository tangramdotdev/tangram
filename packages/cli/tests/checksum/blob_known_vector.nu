use ../../test.nu *

# The sha256 checksum of a blob matches the standard sha256 hash of its contents, formatted as algorithm:hex.

let server = spawn

let contents = "hello, world!\n"
let blob = $contents | tg write

let checksum = tg checksum --algorithm sha256 $blob | from json
let expected = $contents | hash sha256
assert equal $checksum $"sha256:($expected)" "the checksum should match the standard sha256 hash"
