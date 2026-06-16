use ../../test.nu *

# Listing a group's members returns every member.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let carol_user = tg user login carol | from json

tg --token $alice group create team
tg --token $alice group members add team $bob_user.id
tg --token $alice group members add team $carol_user.id

let members = tg --token $alice group members list team | from json
assert ($bob_user.id in $members) "bob should be listed as a member"
assert ($carol_user.id in $members) "carol should be listed as a member"
assert (($members | length) == 2) "the group should have exactly two members"
