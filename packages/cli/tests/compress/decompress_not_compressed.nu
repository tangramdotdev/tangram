use ../../test.nu *

# Decompressing a blob that is not compressed fails with an invalid compression format error in the process log.

let server = spawn

let blob = "hello, world!\n" | tg write

let build = tg decompress --detach --verbose $blob | from json
let wait = tg wait $build.process | from json
assert equal $wait.exit 1 "the decompress process should exit with code 1"

let log = tg log $build.process | complete
assert ($log.stderr | str contains "invalid compression format") "the process log should mention the invalid compression format"
