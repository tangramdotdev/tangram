use ../../test.nu *

# Adding a member that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create team

# Create a group, capture its id, then delete it to obtain a valid but absent id.
let gone = tg --token $alice.token group create disposable | from json
tg --token $alice.token group delete disposable

let output = tg --token $alice.token group members add team $gone.id | complete
failure $output "adding a nonexistent member should fail"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to add the group member
	   group = team
	   member = <group>
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> failed to find the member
	-> failed to find the member

'
