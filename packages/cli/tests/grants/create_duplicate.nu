use ../../test.nu *

# Creating a grant that already exists fails.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let bob_user = tg user login bob | from json

tg --token $alice group create team
tg --token $alice grant $bob_user.id read team

let output = tg --token $alice grant $bob_user.id read team | complete
failure $output "creating a grant that already exists should fail"
assert ($output.stderr | str contains "already") "the error should report that the grant already exists"
