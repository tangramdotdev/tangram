use ../../test.nu *

# An anonymous client cannot claim a group, and a user with no grant cannot create a child of another user's group.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
tg user login eve
let eve = current_token

# An anonymous client cannot claim a top-level group.
let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg group create anon | complete }
failure $output "an anonymous client should not be able to create a group"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"

# Eve has no grant on Alice's group, so she cannot create a child under it.
tg --token $alice group create alice-project
let output = tg --token $eve group create alice-project/eve | complete
failure $output "a user with no grant should not be able to create a child group"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
