use ../lib/test.nu *

# The --executable flag sets the path on the artifact executable rather than resolving the path to the artifact at it.

let local = server spawn

let path = artifact {
	bin: (directory {
		hello: (file --executable '
			#!/bin/sh
			echo hello
		')
	})
}
let id = tg checkin --no-tokens $path | referent node
tg tag put test $id

let process = tg spawn --no-tokens test --executable bin/hello | referent node
let process = tg get $process | from json
let executable = $process.command.node.executable.node
assert equal $executable.artifact $id
assert equal $executable.path "bin/hello"
