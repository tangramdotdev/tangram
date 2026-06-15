use ../../test.nu *

# Adding a member that is already in the organization fails because the membership already exists.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice organization create acme
tg --token $alice organization members add acme $bob_user.id

let output = tg --token $alice organization members add acme $bob_user.id | complete
failure $output "adding a member that already exists should fail"
assert ($output.stderr | str contains "already") "the error should report that the membership already exists"
