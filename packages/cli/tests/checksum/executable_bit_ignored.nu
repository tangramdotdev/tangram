use ../../test.nu *

# A file's executable bit does not affect the checksum of its contents bytes.

let server = server spawn

let file_id = tg put 'tg.file({ "contents": tg.blob("hello"), "executable": false })' | str trim
let executable_id = tg put 'tg.file({ "contents": tg.blob("hello"), "executable": true })' | str trim
let file_checksum = tg checksum $file_id | from json
let executable_checksum = tg checksum $executable_id | from json
assert equal $file_checksum $executable_checksum
