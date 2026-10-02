use ../lib/test.nu *

# A file's dependencies do not affect the checksum of its contents bytes.

let local = server spawn

let file = tg put --no-tokens 'tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "node": tg.file("d") } } })' | referent node
let blob = tg put --no-tokens 'tg.blob("x")' | referent node

let file_checksum = tg checksum $file | from json
let blob_checksum = tg checksum $blob | from json
assert equal $file_checksum $blob_checksum
