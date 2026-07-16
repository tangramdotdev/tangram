use ../../test.nu *

# A user cannot log in with a specifier already claimed by a group.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
tg --token $alice.token group create shared

let output = tg login shared | complete
failure $output "logging in with a specifier claimed by a group should be rejected"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to start the login
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> specifier is already in use
	-> specifier is already in use

'
