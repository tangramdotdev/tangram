use ../../test.nu *

# A group with children cannot be deleted.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create parent/child

let output = tg --token $alice.token group delete parent | complete
failure $output "a group with children should not be deletable"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to delete the group
	   group = parent
	-> the request failed
	   status = 500 Internal Server Error
	-> database error
	-> cannot delete a group with children
	-> cannot delete a group with children

'
