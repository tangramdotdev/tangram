use ../../test.nu *

# Adding a member that does not exist fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create team

# Create a group, capture its id, then delete it to obtain a valid but absent id.
let gone = tg --token $alice group create disposable | from json
tg --token $alice group delete disposable

let output = tg --token $alice group members add team $gone.id | complete
failure $output "adding a nonexistent member should fail"
assert ($output.stderr | str contains "failed to find the member") "the error should mention that the member was not found"
