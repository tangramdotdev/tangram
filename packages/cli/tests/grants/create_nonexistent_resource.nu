use ../../test.nu *

# Granting on a resource that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

let output = tg --token $alice grant $bob_user.id read ghostgroup | complete
failure $output "granting on a nonexistent resource should fail"
assert ($output.stderr | str contains "failed to find the resource") "the error should mention that the resource was not found"
