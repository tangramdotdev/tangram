use ../../test.nu *

# Requesting compression with the zip format fails, because zip archives have their own internal compression.

let server = spawn

let dir = tg put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

let build = tg archive --format zip --compression gz --detach --verbose $dir | from json
let wait = tg wait $build.process | from json
assert equal $wait.exit 1 "the archive process should exit with code 1"

let log = tg log $build.process | complete
assert ($log.stderr | str contains "compression is not supported for zip archives") "the process log should mention that compression is not supported"
