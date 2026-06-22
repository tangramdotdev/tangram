use ../../test.nu *

# A user without read permission cannot get another user's record.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let output = tg --token $eve.token user get $alice.user.id | complete
failure $output "a user without read permission should not be able to get another user"
snapshot ($output.stderr | redact) '
	error an error occurred
	-> failed to find the user
	   user = <user>

'
