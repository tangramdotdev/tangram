use ../../../test.nu *

# tg.encoding.json.decode fails when the input is not valid JSON.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return tg.encoding.json.decode("{ not valid json"); }'
}

let output = tg build $path | complete
failure $output
let stderr = $output.stderr | str replace -ar 'id = (pcs_00[0-9a-z]{26}|[0-9]+)' 'id = PROCESS'
snapshot $stderr '
	error an error occurred
	-> the process failed
	   id = PROCESS
	-> the syscall failed
	   name = encoding_json_decode
	-> failed to decode the string as json
	-> key must be a string at line 1 column 3

'
