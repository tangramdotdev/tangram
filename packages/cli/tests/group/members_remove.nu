use ../../test.nu *

# An admin can remove a member from a group.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team
tg --token $alice group members add team $bob_user.id
tg --token $alice group members remove team $bob_user.id

let members = tg --token $alice group members list team | from json
assert (not ($bob_user.id in $members)) "the removed user should no longer be a member"
