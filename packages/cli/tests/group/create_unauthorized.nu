use ../../test.nu *

# An anonymous client cannot claim a group, and a user with no grant cannot create a child of another user's group.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# An anonymous client cannot claim a top-level group.
let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anon | complete }
failure $output "an anonymous client should not be able to create a group"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the group
	   specifier = anon
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'

# Eve has no grant on Alice's group, so she cannot create a child under it.
tg --token $alice.token group create alice-project
let output = tg --token $eve.token group create alice-project/eve | complete
failure $output "a user with no grant should not be able to create a child group"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to create the group
	   specifier = alice-project/eve
	-> the request failed
	   status = 500 Internal Server Error
	-> unauthorized

'
