use ../../test.nu *

# A malformed permission set is rejected before it reaches the server.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

# Mixing permission kinds in one set is rejected.
let mixed = tg --token $alice.token grant $bob.user.id read,object_node team | complete
failure $mixed "mixing permission kinds in a set should be rejected"
snapshot ($mixed.stderr | redact) r#'
	error: invalid value 'read,object_node' for '<PERMISSIONS>': invalid grant permissions
	
	For more information, try '--help'.

'#

# An unknown permission token is rejected.
let unknown = tg --token $alice.token grant $bob.user.id frobnicate team | complete
failure $unknown "an unknown permission should be rejected"
snapshot ($unknown.stderr | redact) r#'
	error: invalid value 'frobnicate' for '<PERMISSIONS>': invalid grant permission
	
	For more information, try '--help'.

'#
