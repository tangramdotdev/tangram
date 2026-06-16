use ../../test.nu *

# A user cannot log in with a specifier already claimed by a group.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
tg --token $alice group create shared

let output = tg user login shared | complete
failure $output "logging in with a specifier claimed by a group should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
