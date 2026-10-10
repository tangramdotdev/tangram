use ../lib/test.nu *

# A sync get by specifier works when ancestor transfer is disabled.

let local_source = server spawn --name local-source
let remote_destination = server spawn --name remote-destination
let group = tg --url $local_source.url group create foo | from json

# Encode the flow configuration, put node, put end, and sync end for the destination sync stream.
let id = tg id $group.id | into binary
let put_node = (
	0x[0b 01 0b 00 0b 00 0a 03 00 07 14]
	++ $id
	++ 0x[01 06 03 66 6f 6f 03 06 03 66 6f 6f]
)
let input = (
	0x[21 0b 03 0a 04 00 0a 02 00 02 80 80 80 01 01 02 80 08 01 02 80 80 80 20 02 02 80 80 20 03 02 80 80 10]
	++ 0x[2b]
	++ $put_node
	++ 0x[05 0b 01 0b 03 00 05 0b 00 0b 03 00 03 0b 02 00]
)
let socket = $remote_destination.directory | path join socket
let args = [
	'--silent'
	'--show-error'
	'--output' '/dev/null'
	'--unix-socket' $socket
	'--header' 'accept: application/vnd.tangram.sync'
	'--header' 'content-type: application/vnd.tangram.sync'
	'--data-binary' '@-'
	'http://localhost/sync?ancestors=never&get=foo'
]
$input | ^curl ...$args

let actual = tg --url $remote_destination.url group get foo | from json
assert equal $actual.id $group.id
