use ../../test.nu *

# An admin can add a user to a group; the member is listed and gains write on the group.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token

tg --token $alice group create team
tg --token $alice group members add team $bob_user.id

let members = tg --token $alice group members list team | from json
assert ($bob_user.id in $members) "the added user should be listed as a member"

# Membership confers write, so the member can create a subgroup under the team.
tg --token $bob group create team/bob-project
