use ../../lib/test.nu *

# A directory checked in destructively leaves a checkout whose files another user can check in, while preserving their modes and dependencies.
let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let path = artifact {
	data: 'data'
	bin: { run: (file --executable --xattrs { 'user.tangram.dependencies': '["../data"]' } 'run') }
}
chmod 444 ($path | path join data)
chmod 555 ($path | path join bin run)
let modes = ['data', 'bin/run'] | each { |name| ls -l ($path | path join $name) | get mode | first }

let id = tg --token $alice.token checkin --no-tokens --destructive --no-ignore $path | referent node
let checkout = tg --token $alice.token checkout $id | str trim
for name in ['data', 'bin/run'] {
	let path = $checkout | path join $name
	let reference = tg --token $bob.token checkin $path | complete
	success $reference $'another user should be able to check in ($name)'
	success (tg --token $bob.token get --depth=inf ($reference.stdout | str trim) | complete) $'the reference to ($name) should grant its subtree'
}
assert equal (['data', 'bin/run'] | each { |name| ls -l ($checkout | path join $name) | get mode | first }) $modes
assert equal (xattr_read user.tangram.dependencies ($checkout | path join bin run) | from json) ['../data']
server stop $local
