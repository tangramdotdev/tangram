use ../../test.nu *

# Signalling a cacheable process fails because cacheable processes cannot receive signals.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => 42;',
}
let process = tg build --detach $path | str trim
tg wait $process

let output = tg signal $process | complete
failure $output
assert ($output.stderr | str contains 'cannot signal cacheable processes') "the error should explain that cacheable processes cannot be signalled"
