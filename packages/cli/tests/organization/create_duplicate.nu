use ../../test.nu *

# Creating an organization whose specifier is already in use fails with a clear error.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice organization create acme

let output = tg --token $alice organization create acme | complete
failure $output "a duplicate organization should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
