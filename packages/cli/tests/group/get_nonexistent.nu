use ../../test.nu *

# Getting a group that does not exist fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group get ghost | complete
failure $output "getting a nonexistent group should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the group
	   group = ghost
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to list local entries
	-> invalid resource

'
