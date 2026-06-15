use ../../test.nu *

# Adding a member to an organization that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let alice_user = tg user login alice | from json
let alice = current_token

let output = tg --token $alice organization members add ghost $alice_user.id | complete
failure $output "adding a member to a nonexistent organization should fail"
assert ($output.stderr | str contains "failed to find the organization") "the error should mention that the organization was not found"
