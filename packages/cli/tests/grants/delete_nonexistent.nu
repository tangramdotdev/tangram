use ../../test.nu *

# Deleting a grant that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team

let output = tg --token $alice grants delete $bob_user.id read team | complete
failure $output "deleting a grant that does not exist should fail"
assert ($output.stderr | str contains "failed to find the grant") "the error should mention that the grant was not found"
