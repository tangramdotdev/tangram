use ../../test.nu *

# Getting a group that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let output = tg --token $alice group get ghost | complete
failure $output "getting a nonexistent group should fail"
assert ($output.stderr | str contains "failed to find the group") "the error should mention that the group was not found"
