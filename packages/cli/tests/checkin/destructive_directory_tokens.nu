use ../lib/test.nu *

# Destructive directory checkin writes exact file tokens while preserving dependencies and permissions.
let server = server spawn --config { vfs: false }
let path = artifact {
	data: 'data'
	bin: { run: (file --executable --xattrs { 'user.tangram.dependencies': '["../data"]' } 'run') }
}
chmod 444 ($path | path join data)
chmod 555 ($path | path join bin run)
let modes = ['data', 'bin/run'] | each { |name| ls -l ($path | path join $name) | get mode | first }

let id = tg checkin --destructive --no-ignore $path | str trim
let checkout = $server.checkout_directory | path join $id
for name in ['data', 'bin/run'] {
	let path = $checkout | path join $name
	let token = xattr_read user.tangram.token $path
	let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	assert equal $body.resource (tg checkin $path | str trim)
	assert equal $body.permissions [object_subtree]
}
assert equal (['data', 'bin/run'] | each { |name| ls -l ($checkout | path join $name) | get mode | first }) $modes
assert equal (xattr_read user.tangram.dependencies ($checkout | path join bin run) | from json) ['../data']
server stop $server
