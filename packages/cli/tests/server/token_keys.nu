use ../../test.nu *

# Default token keys are private to their server directory and survive graceful and abrupt restarts.

let server = server spawn --config { vfs: false }
let keys = [authentication authorization] | each { |kind|
	let path = $server.directory | path join $'($kind).key'
	assert equal (ls -l $path | get mode | first) 'rw-------'
	let bytes = open --raw $path | into binary
	assert equal ($bytes | bytes length) 32
	{ kind: $kind, hash: ($bytes | hash sha256) }
}
assert not equal $keys.0.hash $keys.1.hash

let server = server restart $server
for key in $keys {
	let path = $server.directory | path join $'($key.kind).key'
	assert equal (open --raw $path | hash sha256) $key.hash
}

let pid = open ($server.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_until { ps | where pid == $pid | is-empty } 'the server must stop'
let server = server start $server
for key in $keys {
	let path = $server.directory | path join $'($key.kind).key'
	assert equal (open --raw $path | hash sha256) $key.hash
}
server stop $server

let other = server spawn --config { vfs: false }
for key in $keys {
	let path = $other.directory | path join $'($key.kind).key'
	assert not equal (open --raw $path | hash sha256) $key.hash
}
