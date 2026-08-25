use ../../test.nu *

# Adding a member grants that member write on the organization, so the operation requires admin: a write user cannot add members.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let carol = tg login --verbose --name carol | from json

tg --token $alice.token organization create acme
tg --token $alice.token grant $eve.user.id write acme

# Eve has write but not admin, so she cannot add an accomplice to the organization.
let output = tg --token $eve.token organization members add acme $carol.user.id | complete
failure $output "a write user should not be able to add a member"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the organization member
	   member = usr_0000000000000000000000000000
	   organization = acme
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
