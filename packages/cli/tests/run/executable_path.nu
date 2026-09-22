use ../lib/test.nu *

# The --executable flag sets the path on the artifact executable rather than resolving the path to the artifact at it.

let server = server spawn

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
let executable = $process.command.node.executable.node
assert equal $executable.artifact $id
assert equal $executable.path "bin/hello"
