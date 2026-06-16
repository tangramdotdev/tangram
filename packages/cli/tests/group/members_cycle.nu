use ../../test.nu *

# A group cannot be added as a member of one of its own members, which would form a cycle.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let a = tg --token $alice group create a | from json
let b = tg --token $alice group create b | from json

# b is a member of a.
tg --token $alice group members add a $b.id

# Adding a as a member of b would create a cycle.
let output = tg --token $alice group members add b $a.id | complete
failure $output "adding a group to its own member should be rejected"
assert ($output.stderr | str contains "membership cycle") "the error should mention a membership cycle"
