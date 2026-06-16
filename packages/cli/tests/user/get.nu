use ../../test.nu *

# A user can get their own record by id and by specifier.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

let user = tg user login alice | from json
let alice = current_token

let by_id = tg --token $alice user get $user.id | from json
assert ($by_id.id == $user.id) "getting a user by id should return that user"

let by_specifier = tg --token $alice user get alice | from json
assert ($by_specifier.id == $user.id) "getting a user by specifier should return that user"
