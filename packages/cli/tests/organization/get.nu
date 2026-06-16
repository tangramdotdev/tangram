use ../../test.nu *

# An organization can be retrieved by its id and by its specifier.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

let organization = tg --token $alice organization create acme | from json

let by_id = tg --token $alice organization get $organization.id | from json
assert ($by_id.id == $organization.id) "getting an organization by id should return that organization"

let by_specifier = tg --token $alice organization get acme | from json
assert ($by_specifier.id == $organization.id) "getting an organization by specifier should return that organization"
