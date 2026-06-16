use ../../test.nu *

# Write on an organization does not confer admin, so a write user cannot delete it.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token

tg --token $alice organization create acme
tg --token $alice grant $bob_user.id write acme

# Bob's write lets him tag under the organization.
let id = tg --token $bob checkin (artifact 'x')
tg --token $bob tag acme/foo $id

# But write does not confer admin, so bob cannot delete the organization.
let output = tg --token $bob organization delete acme | complete
failure $output "a write user should not be able to delete the organization"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
