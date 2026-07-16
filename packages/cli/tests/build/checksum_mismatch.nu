use ../../test.nu *

# Building with a checksum flag that does not match the output fails with a checksum mismatch.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.file("hello"); }'
}

let output = tg build --checksum "sha256:0000000000000000000000000000000000000000000000000000000000000000" $path | complete
failure $output
snapshot --normalize-ids --redact $path $output.stderr '
	error an error occurred
	-> the process failed
	   id = pcs_0000000000000000000000000000
	-> checksum mismatch
	   actual = sha256:4bc678d476f1906a0e3e5e84f9d02d34957f3913bd5e5eb35cd3efa28ff80f40
	   expected = sha256:0000000000000000000000000000000000000000000000000000000000000000

'
