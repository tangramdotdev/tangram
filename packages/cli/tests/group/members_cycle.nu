use ../../test.nu *

# A group cannot be added as a member of one of its own members, which would form a cycle.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let a = tg --token $alice.token group create a | from json
let b = tg --token $alice.token group create b | from json

# b is a member of a.
tg --token $alice.token group members add a $b.id

# Adding a as a member of b would create a cycle.
let output = tg --token $alice.token group members add b $a.id | complete
failure $output "adding a group to its own member should be rejected"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to add the group member
	   group = b
	   member = grp_0000000000000000000000000000
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> membership cycle
	-> membership cycle

'
