use ../../test.nu *

# Deleting a leaf group removes it.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create leaf
tg --token $alice group delete leaf

let output = tg --token $alice group get leaf | complete
failure $output "the group should not exist after deletion"
assert ($output.stderr | str contains "failed to find the group") "the deleted group should no longer be found"
