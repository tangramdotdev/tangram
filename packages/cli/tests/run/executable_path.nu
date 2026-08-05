use ../../test.nu *

# The --executable flag sets the path on the artifact executable rather than resolving the path to the artifact at it.

let server = spawn

let path = artifact {
	bin: (directory {
		hello: (file --executable '
			#!/bin/sh
			echo hello
		')
	})
}
let id = tg checkin $path | str trim
tg tag put test $id

let process = tg spawn test --executable bin/hello | str trim
let process = tg get $process | from json
let command = tg get $process.command | str trim
assert ($command | str contains $'"artifact":($id)') $"expected the tagged directory, got ($command)"
assert ($command | str contains '"path":"bin/hello"') $"expected the path to be set, got ($command)"
