use ../../test.nu *

# The artifact checksum encodes structure, so changing the executable bit or an entry name changes the checksum.

let server = spawn

let file_id = tg put 'tg.file({ "contents": tg.blob("hello"), "executable": false })' | str trim
let executable_id = tg put 'tg.file({ "contents": tg.blob("hello"), "executable": true })' | str trim
let file_checksum = tg checksum $file_id | from json
let executable_checksum = tg checksum $executable_id | from json
assert ($file_checksum != $executable_checksum) "the executable bit should change the checksum"

let dir_a = tg put 'tg.directory({ "a": tg.file("x") })' | str trim
let dir_b = tg put 'tg.directory({ "b": tg.file("x") })' | str trim
let checksum_a = tg checksum $dir_a | from json
let checksum_b = tg checksum $dir_b | from json
assert ($checksum_a != $checksum_b) "the entry name should change the checksum"
