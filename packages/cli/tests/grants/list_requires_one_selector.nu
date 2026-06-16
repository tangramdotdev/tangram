use ../../test.nu *

# Listing grants requires exactly one of a resource or a principal.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let output = tg --token $alice grants list | complete
failure $output "listing grants without a selector should fail"
assert ($output.stderr | str contains "expected exactly one of a resource or a principal") "the error should mention that exactly one selector is required"
