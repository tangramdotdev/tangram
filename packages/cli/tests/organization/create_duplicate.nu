use ../../test.nu *

# Creating an organization whose specifier is already in use fails with a clear error.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

tg --token $alice.token organization create acme

let output = tg --token $alice.token organization create acme | complete
failure $output "a duplicate organization should be rejected"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the organization
	   specifier = acme
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> specifier is already in use
	-> specifier is already in use

'
