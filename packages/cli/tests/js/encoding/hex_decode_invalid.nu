use ../../../test.nu *

# tg.encoding.hex.decode fails when the input is not valid lowercase hexadecimal.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => tg.encoding.hex.decode("xyz");'
}

let output = tg build $path | complete
failure $output
let stderr = $output.stderr | str replace -ar 'id = (pcs_00[0-9a-z]{26}|[0-9]+)' 'id = PROCESS'
snapshot $stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> the syscall failed
	   name = encoding_hex_decode
	-> failed to decode the string as hex
	-> invalid length at 2

'
