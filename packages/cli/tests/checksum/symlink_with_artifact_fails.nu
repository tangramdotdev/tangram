use ../../test.nu *

# Checksumming a directory that contains a symlink with an artifact target fails.

let server = spawn

let dir = tg put 'tg.directory({ "link": tg.symlink({ "artifact": tg.file("target") }) })' | str trim

let build = tg checksum --detach --verbose $dir | from json
let wait = tg wait $build.process | from json
assert equal $wait.exit 1 "the checksum process should exit with code 1"

let log = tg log $build.process | complete
snapshot ($log.stderr | redact) '
	 0 B
	-> cannot checksum a symlink with an artifact

'
