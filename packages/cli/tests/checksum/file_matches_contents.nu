use ../../test.nu *

# The checksum of a file is the checksum of its contents bytes.

let server = server spawn

let file_id = tg put 'tg.file("hello")' | str trim
let blob_id = tg put 'tg.blob("hello")' | str trim

let file_checksum = tg checksum $file_id | from json
let blob_checksum = tg checksum $blob_id | from json
assert equal $file_checksum $blob_checksum
