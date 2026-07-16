use ../../test.nu *

# A sandbox must not be created with a public owner.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let create = tg --token $alice.token sandbox create --owner public --no-network | complete
failure $create "a sandbox must not be created with a public owner"
snapshot --normalize $create.stderr '
	error an error occurred
	-> invalid sandbox owner

'
