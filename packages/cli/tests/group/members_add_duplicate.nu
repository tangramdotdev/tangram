use ../../test.nu *

# Adding a member that is already in the group fails because the membership already exists.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

let output = tg --token $alice.token group members add team $bob.user.id | complete
failure $output "adding a member that already exists should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the group member
	   group = team
	   member = usr_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> the member is already in the group
	-> the member is already in the group

'
