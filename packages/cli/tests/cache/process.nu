use ../../test.nu *

# Caching a process fails because a process is not an object.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "hello";'
}
let process = tg build --detach $path | str trim
tg wait $process

let output = tg cache $process | complete
failure $output
assert ($output.stderr | str contains "expected an object ID") "the error should mention that an object id was expected"
