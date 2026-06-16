use ../../test.nu *

# An organization with children cannot be deleted.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice organization create acme

# Tagging under the organization gives it a child.
let id = tg --token $alice checkin (artifact 'x')
tg --token $alice tag acme/foo $id

let output = tg --token $alice organization delete acme | complete
failure $output "an organization with children should not be deletable"
assert ($output.stderr | str contains "cannot delete an organization with children") "the error should mention that the organization has children"
