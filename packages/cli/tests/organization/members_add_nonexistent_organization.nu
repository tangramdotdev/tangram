use ../../test.nu *

# Adding a member to an organization that does not exist fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization members add ghost $alice.user.id | complete
failure $output "adding a member to a nonexistent organization should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the organization member
	   member = usr_0000000000000000000000000000
	   organization = ghost
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the organization

'
