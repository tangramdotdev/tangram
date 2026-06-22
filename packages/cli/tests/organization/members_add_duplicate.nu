use ../../test.nu *

# Adding a member that is already in the organization fails because the membership already exists.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id

let output = tg --token $alice.token organization members add acme $bob.user.id | complete
failure $output "adding a member that already exists should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to add the organization member
	   member = <user>
	   organization = acme
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> the member is already in the organization
	-> the member is already in the organization

'
