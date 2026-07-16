use ../../test.nu *

# Granting on a resource that does not exist fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let output = tg --token $alice.token grant $bob.user.id read ghostgroup | complete
failure $output "granting on a nonexistent resource should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the grant
	   principal = usr_0000000000000000000000000000
	   resource = ghostgroup
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the resource

'
