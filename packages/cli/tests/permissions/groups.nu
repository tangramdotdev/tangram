use ../../test.nu *

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

def assert_unauthorized [output: record, message: string] {
	failure $output $message
	assert ($output.stderr | str contains "unauthorized") "The error should mention that the request is unauthorized."
}

tg user login alice
let alice = current_token
tg user login bob
let bob = current_token
tg user login carol
let carol = current_token

tg --token $alice group create team

let output = tg --token $bob namespace create team/project | complete
assert_unauthorized $output "Bob should not inherit the team's permissions before becoming a member."

let output = tg --token $bob group member add team bob | complete
assert_unauthorized $output "Bob should not be able to add himself to the team."

let output = tg --token $bob group grants team | complete
assert_unauthorized $output "Bob should not be able to inspect the team's grants before becoming a member."

tg --token $alice group member add team bob
tg --token $bob group grants team
tg --token $bob namespace create team/project
tg --token $bob group member add team carol
tg --token $carol namespace create team/carol

# Nested groups.
tg --token $alice group create company

let output = tg --token $carol group create company/team | complete
assert_unauthorized $output "Carol should not be able to create a subgroup without admin on the parent namespace."

tg --token $alice group create company/team

let output = tg --token $carol namespace create company/team/project | complete
assert_unauthorized $output "Carol should not inherit the subgroup's permissions before becoming a member."

tg --token $alice group member add company/team carol
tg --token $carol namespace create company/team/project
