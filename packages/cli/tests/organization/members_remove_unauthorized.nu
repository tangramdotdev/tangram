use ../../test.nu *

# Membership confers write but not admin, so a member cannot remove another member.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token
let carol_user = tg user login carol | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id
tg --token $alice organization members add acme $carol_user.id

# Bob's membership grants him write but not admin, so he cannot remove another member.
let output = tg --token $bob organization members remove acme $carol_user.id | complete
failure $output "a member without admin should not be able to remove another member"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
