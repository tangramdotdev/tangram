use ../../test.nu *

# Adding a member to a group that does not exist fails.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group members add ghost $alice.user.id | complete
failure $output "adding a member to a nonexistent group should fail"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the group member
	   group = ghost
	   member = usr_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> failed to find the group

'
