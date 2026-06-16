use ../../test.nu *

# A group with children cannot be deleted.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create parent/child

let output = tg --token $alice group delete parent | complete
failure $output "a group with children should not be deletable"
assert ($output.stderr | str contains "cannot delete a group with children") "the error should mention that the group has children"
