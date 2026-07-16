use ../../test.nu *

# An organization is flat, so a multi-component specifier is rejected.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization create acme/sub | complete
failure $output "a multi-component organization specifier should be rejected"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the organization
	   specifier = acme/sub
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> invalid organization specifier
	-> invalid organization specifier

'
