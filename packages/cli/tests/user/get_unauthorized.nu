use ../../test.nu *

# A user without read permission cannot get another user's record.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let alice_user = tg user login alice | from json
tg user login eve
let eve = current_token

let output = tg --token $eve user get $alice_user.id | complete
failure $output "a user without read permission should not be able to get another user"
assert ($output.stderr | str contains "failed to find the user") "the user should not be visible without read permission"
