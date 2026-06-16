use ../../test.nu *

# Getting an organization that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let output = tg --token $alice organization get ghost | complete
failure $output "getting a nonexistent organization should fail"
assert ($output.stderr | str contains "failed to find the organization") "the error should mention that the organization was not found"
