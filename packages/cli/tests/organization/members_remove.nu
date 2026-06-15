use ../../test.nu *

# An admin can remove a member from an organization.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id
tg --token $alice organization members remove acme $bob_user.id

let members = tg --token $alice organization members list acme | from json
assert (not ($bob_user.id in $members)) "the removed user should no longer be a member"
