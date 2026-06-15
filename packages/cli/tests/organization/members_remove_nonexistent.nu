use ../../test.nu *

# Removing a user that is not a member of the organization fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice organization create acme

let output = tg --token $alice organization members remove acme $bob_user.id | complete
failure $output "removing a user that is not a member should fail"
assert ($output.stderr | str contains "failed to find the organization member") "the error should mention that the organization member was not found"
