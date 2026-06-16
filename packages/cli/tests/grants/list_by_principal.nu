use ../../test.nu *

# A user can list their own grants but not another user's grants.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let alice_user = tg user login alice | from json
tg user login eve
let eve = current_token
let eve_user = tg user whoami | from json

# Eve can list her own grants (she holds admin on herself from login).
let own = tg --token $eve grants list --principal $eve_user.id | from json
assert (($own | length) > 0) "a user should be able to list their own grants"

# Eve cannot list Alice's grants without admin on Alice.
let output = tg --token $eve grants list --principal $alice_user.id | complete
failure $output "a user should not be able to list another user's grants"
assert ($output.stderr | str contains "failed to find the principal") "the principal should not be visible without read permission"
