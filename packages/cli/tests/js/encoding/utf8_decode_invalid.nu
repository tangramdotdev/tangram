use ../../../test.nu *

# tg.encoding.utf8.decode fails when the byte array is not valid UTF-8.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.utf8.decode(new Uint8Array([0xff])); }'
}

let output = tg build $path | complete
failure $output
let stderr = $output.stderr | str replace -ar 'id = (pcs_00[0-9a-z]{26}|[0-9]+)' 'id = PROCESS'
snapshot $stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> the syscall failed
	   name = encoding_utf8_decode
	-> failed to decode the bytes as UTF-8
	-> invalid utf-8 sequence of 1 bytes from index 0

'
