use ../../test.nu *

# A group cannot be created with a specifier that is already in use.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create project
let output = tg --token $alice.token group create project | complete
failure $output "creating a duplicate group should be rejected"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to create the group
	   specifier = project
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> specifier is already in use
	-> specifier is already in use

'
