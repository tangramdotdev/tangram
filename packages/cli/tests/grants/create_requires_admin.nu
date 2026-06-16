use ../../test.nu *

# Granting a permission on a resource requires admin on it, so a write user cannot grant.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token
let eve_user = tg user login eve | from json
let eve = current_token
let carol_user = tg user login carol | from json

tg --token $alice group create team
tg --token $alice grant $eve_user.id write team

# Eve has write but not admin, so she cannot hand access to a third party.
let output = tg --token $eve grant $carol_user.id read team | complete
failure $output "a user without admin should not be able to grant"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
