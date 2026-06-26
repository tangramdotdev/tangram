use ../../test.nu *

# Adding a member grants that member write on the organization, so the operation requires admin: a write user cannot add members.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token organization create acme
tg --token $alice.token grant $eve.user.id write acme

# Eve has write but not admin, so she cannot add an accomplice to the organization.
let output = tg --token $eve.token organization members add acme $carol.user.id | complete
failure $output "a write user should not be able to add a member"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to add the organization member
	   member = <user>
	   organization = acme
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
