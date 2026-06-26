use ../../test.nu *

# A permission whose kind does not match the resource kind is rejected.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

# An object permission cannot be attached to a group resource.
let output = tg --token $alice.token grant $bob.user.id object_node team | complete
failure $output "an object permission should not be grantable on a group"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = team
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> invalid permission for the resource
	-> invalid permission for the resource

'
