use ../../test.nu *

# Listing an organization's members returns every member.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let carol_user = tg user login carol | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id
tg --token $alice organization members add acme $carol_user.id

let members = tg --token $alice organization members list acme | from json
assert ($bob_user.id in $members) "bob should be listed as a member"
assert ($carol_user.id in $members) "carol should be listed as a member"
assert (($members | length) == 2) "the organization should have exactly two members"
