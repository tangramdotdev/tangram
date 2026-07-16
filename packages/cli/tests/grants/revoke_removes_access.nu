use ../../test.nu *

# Revoking a grant immediately removes the principal's access, not just the grant record.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create resource

# Bob can read the resource while the grant exists.
tg --token $alice.token grant $bob.user.id read resource
tg --token $bob.token group get resource

# After revoking the grant, bob loses access entirely.
tg --token $alice.token revoke $bob.user.id read resource
let output = tg --token $bob.token group get resource | complete
failure $output "revoking the grant should remove the principal's access"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to find the group

'
