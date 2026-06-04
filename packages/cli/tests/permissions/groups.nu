use ../../test.nu *

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

def assert_unauthorized [output: record, message: string] {
	failure $output $message
	assert ($output.stderr | str contains "unauthorized") "The error should mention that the request is unauthorized."
}

let alice_user = tg user login alice | from json
let alice = current_token
let bob_user = tg user login bob | from json
let bob = current_token
let carol_user = tg user login carol | from json
let carol = current_token

let team = tg --token $alice group create team | from json

let output = tg --token $bob group create team/project | complete
assert_unauthorized $output "Bob should not inherit the team's permissions before becoming a member."

let output = tg --token $bob group members add team $bob_user.id | complete
assert_unauthorized $output "Bob should not be able to add himself to the team."

let output = tg --token $bob group grants team | complete
assert_unauthorized $output "Bob should not be able to inspect the team's grants before becoming a member."

tg --token $alice group members add team $bob_user.id
tg --token $bob group grants team
tg --token $bob group create team/project
tg --token $bob group members add team $carol_user.id
tg --token $carol group create team/carol

# Nested groups.
tg --token $alice group create company

let output = tg --token $carol group create company/team | complete
assert_unauthorized $output "Carol should not be able to create a subgroup without admin on the parent group."

tg --token $alice group create company/team

let output = tg --token $carol group create company/team/project | complete
assert_unauthorized $output "Carol should not inherit the subgroup's permissions before becoming a member."

tg --token $alice group members add company/team $carol_user.id
tg --token $carol group create company/team/project
