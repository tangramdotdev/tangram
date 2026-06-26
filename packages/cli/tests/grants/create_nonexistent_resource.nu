use ../../test.nu *

# Granting on a resource that does not exist fails.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let output = tg --token $alice.token grant $bob.user.id read ghostgroup | complete
failure $output "granting on a nonexistent resource should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to create the grant
	   principal = <user>
	   resource = ghostgroup
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'
