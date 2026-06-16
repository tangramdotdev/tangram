use ../../test.nu *

# An organization is flat, so a multi-component specifier is rejected.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let output = tg --token $alice organization create acme/sub | complete
failure $output "a multi-component organization specifier should be rejected"
assert ($output.stderr | str contains "invalid organization specifier") "the error should mention an invalid organization specifier"
