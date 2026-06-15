use ../../test.nu *

# Adding a member grants that member write on the group, so the operation requires admin: a write user cannot add members.

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

# Eve has write but not admin, so she cannot add an accomplice to the group.
let output = tg --token $eve group members add team $carol_user.id | complete
failure $output "a write user should not be able to add a member"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
