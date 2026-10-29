use ../../lib/test.nu *

# Another user can check in a file that its owner checked out, although the user cannot read the file by its ID.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	remotes: {}
	vfs: false
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

let id = tg --token $alice.token put --no-tokens 'tg.file("contents")' | referent node
tg index
let path = tg --token $alice.token checkout $id | str trim

# Bob cannot read Alice's private file by its ID.
let output = tg --token $bob.token get --bytes $id | complete
failure $output 'reading without the token should fail'

# Bob can check in the checked out file.
let output = tg --token $bob.token checkin --no-tokens $path | complete
success $output 'checkin should authorize the checked out file'
snapshot $output.stderr ''
assert equal ($output.stdout | referent node) $id
