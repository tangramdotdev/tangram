use ../../test.nu *

# Write on an organization does not confer admin, so a write user cannot delete it.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token grant $bob.user.id write acme

# Bob's write lets him tag under the organization.
let id = tg --token $bob.token checkin (artifact 'x')
tg --token $bob.token tag acme/foo $id

# But write does not confer admin, so bob cannot delete the organization.
let output = tg --token $bob.token organization delete acme | complete
failure $output "a write user should not be able to delete the organization"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to delete the organization
	   organization = acme
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
