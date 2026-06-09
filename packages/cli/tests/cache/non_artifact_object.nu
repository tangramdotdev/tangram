use ../../test.nu *

# Caching an object that is not an artifact fails.

let server = spawn

# Build a process so we can reference its command, which is a non-artifact object.
let path = artifact {
	tangram.ts: 'export default () => "hello";'
}
let process = tg build --detach $path | str trim
tg wait $process
let command = tg get $process | from json | get command

let output = tg cache $command | complete
failure $output
assert ($output.stderr | str contains "expected an artifact") "the error should mention that an artifact was expected"
