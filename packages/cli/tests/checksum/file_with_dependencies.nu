use ../../test.nu *

# A file's dependencies do not affect the checksum of its contents bytes.

let server = spawn

let file = tg put 'tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "node": tg.file("d") } } })' | str trim
let blob = tg put 'tg.blob("x")' | str trim

let file_checksum = tg checksum $file | from json
let blob_checksum = tg checksum $blob | from json
assert equal $file_checksum $blob_checksum
