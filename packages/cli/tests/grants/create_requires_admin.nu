use ../../test.nu *

# Granting a permission on a resource requires admin on it, so a write user cannot grant.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $eve.user.id write team

# Eve has write but not admin, so she cannot hand access to a third party.
let output = tg --token $eve.token grant $carol.user.id read team | complete
failure $output "a user without admin should not be able to grant"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the grant
	   principal = usr_0000000000000000000000000000
	   resource = team
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
