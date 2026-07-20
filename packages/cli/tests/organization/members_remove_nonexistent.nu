use ../../test.nu *

# Removing a user that is not a member of the organization fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme

let output = tg --token $alice.token organization members remove acme $bob.user.id | complete
failure $output "removing a user that is not a member should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the organization member

'
