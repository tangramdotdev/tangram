use ../../test.nu *

# Waiting on an already finished process returns the same outcome on every call.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => 42;',
}
let process = tg build --detach $path | str trim

let first = tg wait $process | from json
let second = tg wait $process | from json
assert ($first == $second) "repeated waits should return the same outcome"
assert ($first.exit == 0) "the wait outcome should report a successful exit"
assert ($first.output == 42) "the wait outcome should carry the process output"
