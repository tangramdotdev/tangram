use ../../test.nu *

# Creating a grant that already exists fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read team

let output = tg --token $alice.token grant $bob.user.id read team | complete
failure $output "creating a grant that already exists should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the grant
	   principal = usr_0000000000000000000000000000
	   resource = team
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> the grant already exists
	-> the grant already exists

'
