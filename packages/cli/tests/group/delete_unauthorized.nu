use ../../test.nu *

# Membership confers write but not admin, so a member cannot delete the group.

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

# Bob's membership grants him write, so he can create a subgroup under the team.
tg --token $bob group create team/bob-sub

# Write does not confer admin, so bob cannot delete the group.
let output = tg --token $bob group delete team | complete
failure $output "a member without admin should not be able to delete the group"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
