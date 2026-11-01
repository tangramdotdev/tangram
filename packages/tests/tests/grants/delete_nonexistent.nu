use ../../util/test.nu *

# Deleting a grant that does not exist fails.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

tg --token $alice.token group create team

let output = tg --token $alice.token revoke $bob.user.id read team | complete
failure $output "revoking a grant that does not exist should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to revoke the grant
	   resource = team
	   subject = usr_0000000000000000000000000000
	-> the request failed
	   status = 404 Not Found
	-> failed to find the grant

'
