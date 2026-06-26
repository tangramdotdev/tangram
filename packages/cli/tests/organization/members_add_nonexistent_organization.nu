use ../../test.nu *

# Adding a member to an organization that does not exist fails.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization members add ghost $alice.user.id | complete
failure $output "adding a member to a nonexistent organization should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to add the organization member
	   member = <user>
	   organization = ghost
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the organization

'
