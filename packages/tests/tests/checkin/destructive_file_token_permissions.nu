use ../../lib/test.nu *

# Without solving, a destructively checked-in file with an unproven dependency leaves a checkout that another user cannot check in.
let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let dependency = tg --token $alice.token put --no-tokens 'tg.file("dependency")' | referent node
let path = artifact (file --xattrs { 'user.tangram.dependencies': ([$dependency] | to json) } 'output')
let id = tg --token $alice.token checkin --no-tokens --destructive --no-ignore --no-solve $path | referent node
let checkout = tg --token $alice.token checkout $id | str trim
failure (tg --token $bob.token checkin --no-tokens $checkout | complete) 'the checkout should not grant the unproven dependency'
server stop $local
