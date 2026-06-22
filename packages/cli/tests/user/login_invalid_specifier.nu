use ../../test.nu *

# A user specifier must be a single component, so a multi-component login is rejected.

let server = spawn --config { authentication: true }

let output = tg login "alice/bob" | complete
failure $output "a multi-component user specifier should be rejected"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to log in
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> invalid user specifier
	-> invalid user specifier

'
