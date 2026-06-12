use ../../../test.nu *

# tg.encoding.base64.decode fails when the input contains a character outside the base64 alphabet.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.encoding.base64.decode("!!!notbase64");'
}

let output = tg build $path | complete
failure $output
let stderr = $output.stderr | str replace -ar 'id = (pcs_00[0-9a-z]{26}|[0-9]+)' 'id = PROCESS'
snapshot $stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> the syscall failed
	   name = encoding_base64_decode
	-> failed to decode the bytes
	-> invalid symbol at 0

'
