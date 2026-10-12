use ../lib/test.nu *

# Request and response encodings are independent, including the framed arg and header.
const helper = path self ../lib/sync_serialization.py
let source = server spawn --name source
let group = tg --url $source.url group create foo | from json
let id_hex = tg id $group.id | into binary | encode hex
for input in [json tangram] {
	for output in [json tangram] {
		let destination = server spawn --name $'destination-($input)-($output)'
		let socket = $destination.directory | path join socket
		let response = python3 $helper $socket $group.id $id_hex $input $output | complete
		success $response 'the sync should support independent request and response encodings'
		let actual = tg --url $destination.url group get foo | from json
		assert equal $actual.id $group.id
	}
}
