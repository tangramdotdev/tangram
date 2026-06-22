use ../../test.nu *

# Building with a checksum flag that does not match the output fails with a checksum mismatch.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.file("hello");'
}

let output = tg build --checksum "sha256:0000000000000000000000000000000000000000000000000000000000000000" $path | complete
failure $output
snapshot ($output.stderr | redact $path | normalize_ids) '
	error an error occurred
	-> the process failed
	   id = <process>
	-> checksum mismatch
	   actual = sha256:4bc678d476f1906a0e3e5e84f9d02d34957f3913bd5e5eb35cd3efa28ff80f40
	   expected = sha256:0000000000000000000000000000000000000000000000000000000000000000

'
