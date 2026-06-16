use ../../test.nu *

# A group cannot be created with a specifier that is already in use.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create project
let output = tg --token $alice group create project | complete
failure $output "creating a duplicate group should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
