use ../../test.nu *

# Adding a member grants that member write on the group, so the operation requires admin: a write user cannot add members.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let carol = tg login --verbose --name carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $eve.user.id write team

# Eve has write but not admin, so she cannot add an accomplice to the group.
let output = tg --token $eve.token group members add team $carol.user.id | complete
failure $output "a write user should not be able to add a member"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the group member
	   group = team
	   member = usr_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
