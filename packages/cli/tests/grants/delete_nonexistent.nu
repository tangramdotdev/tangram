use ../../test.nu *

# Deleting a grant that does not exist fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

let output = tg --token $alice.token grants delete $bob.user.id read team | complete
failure $output "deleting a grant that does not exist should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the grant

'
