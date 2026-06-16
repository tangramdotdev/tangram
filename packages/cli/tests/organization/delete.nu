use ../../test.nu *

# Deleting an organization removes it.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice organization create acme
tg --token $alice organization delete acme

let output = tg --token $alice organization get acme | complete
failure $output "the organization should not exist after deletion"
assert ($output.stderr | str contains "failed to find the organization") "the deleted organization should no longer be found"
