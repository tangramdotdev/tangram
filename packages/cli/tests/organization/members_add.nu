use ../../test.nu *

# An admin can add a member to an organization, and the member is listed.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id

let members = tg --token $alice organization members list acme | from json
assert ($bob_user.id in $members) "the added user should be listed as a member"
