use ../lib/test.nu *

# The checksum of a file is the checksum of its contents bytes.

let local = server spawn

let file_id = tg put --no-tokens 'tg.file("hello")' | referent node
let blob_id = tg put --no-tokens 'tg.blob("hello")' | referent node

let file_checksum = tg checksum $file_id | from json
let blob_checksum = tg checksum $blob_id | from json
assert equal $file_checksum $blob_checksum
