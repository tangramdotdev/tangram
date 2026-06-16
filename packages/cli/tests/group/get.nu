use ../../test.nu *

# A group can be retrieved by its id and by its specifier.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let group = tg --token $alice group create project | from json

let by_id = tg --token $alice group get $group.id | from json
assert ($by_id.id == $group.id) "getting a group by id should return that group"

let by_specifier = tg --token $alice group get project | from json
assert ($by_specifier.id == $group.id) "getting a group by specifier should return that group"
