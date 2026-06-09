use ../../test.nu *

# Building with a checksum flag that does not match the output fails with a checksum mismatch.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("hello");'
}

let output = tg build --checksum "sha256:0000000000000000000000000000000000000000000000000000000000000000" $path | complete
failure $output
assert ($output.stderr | str contains "checksum mismatch") "the error should mention the checksum mismatch"
