use ../../test.nu *

# The checksum of a file artifact uses the artifact encoding, so it differs from the checksum of its contents blob.

let server = spawn

let file_id = tg put 'tg.file("hello")' | str trim
let blob_id = tg put 'tg.blob("hello")' | str trim

let file_checksum = tg checksum $file_id | from json
let blob_checksum = tg checksum $blob_id | from json
assert ($file_checksum != $blob_checksum) "the file artifact checksum should differ from the contents blob checksum"
