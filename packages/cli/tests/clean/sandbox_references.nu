use ../../test.nu *

# Cleaning retains a destroyed sandbox while a process references it and removes the sandbox after the process is cleaned.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("output");
		}
	'
}

let process = tg process spawn --sandbox $path | str trim
tg wait $process

let sandbox = tg process get $process | from json | get sandbox
tg wait $sandbox

# A tagged process keeps its sandbox alive.
tg tag process $process
tg clean
tg sandbox get $sandbox

# Removing the tag allows the process and then its sandbox to be cleaned.
tg tag delete process
tg clean
failure (tg sandbox get $sandbox | complete)
