use ../../test.nu *

# Revoking a grant immediately removes the principal's access, not just the grant record.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token

tg --token $alice group create resource

# Bob can read the resource while the grant exists.
tg --token $alice grant $bob_user.id read resource
tg --token $bob group get resource

# After revoking the grant, bob loses access entirely.
tg --token $alice revoke $bob_user.id read resource
let output = tg --token $bob group get resource | complete
failure $output "revoking the grant should remove the principal's access"
assert ($output.stderr | str contains "failed to find the group") "the resource should be invisible after revoke"
