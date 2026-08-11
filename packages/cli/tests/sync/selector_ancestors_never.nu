use ../../test.nu *

# A sync get by specifier works when ancestor transfer is disabled.

let source = spawn --name source
let destination = spawn --name destination
let group = tg --url $source.url group create foo | from json

# Encode a put node, put end, and sync end for the destination sync stream.
let id = tg id $group.id | into binary
let put_node = (
	0x[0b 01 0b 00 0b 00 0a 03 00 07 14]
	++ $id
	++ 0x[01 06 03 66 6f 6f 03 06 03 66 6f 6f]
)
let input = (
	0x[2b]
	++ $put_node
	++ 0x[05 0b 01 0b 03 00 05 0b 00 0b 03 00 03 0b 02 00]
)
let socket = $destination.directory | path join socket
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

let actual = tg --url $destination.url group get foo | from json
assert equal $actual.id $group.id
