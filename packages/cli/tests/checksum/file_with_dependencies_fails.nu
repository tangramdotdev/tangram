use ../../test.nu *

# Checksumming a directory that contains a file with dependencies fails.

let server = spawn

let dir = tg put 'tg.directory({ "f": tg.file({ "contents": tg.blob("x"), "dependencies": { "dep": { "item": tg.file("d") } } }) })' | str trim

let build = tg checksum --detach --verbose $dir | from json
let wait = tg wait $build.process | from json
assert equal $wait.exit 1 "the checksum process should exit with code 1"

let log = tg log $build.process | complete
snapshot ($log.stderr | redact) '
	 0 B
	-> cannot checksum a file with dependencies

'
