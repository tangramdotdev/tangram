use ../../test.nu *

# Listing grants requires exactly one of a resource or a principal.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token grants list | complete
failure $output "listing grants without a selector should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to list the grants
	-> the request failed
	   status = 500 Internal Server Error
	-> expected exactly one of a resource or a principal

'
