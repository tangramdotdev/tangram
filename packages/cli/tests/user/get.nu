use ../../test.nu *

# A user can get their own record by id and by specifier.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let by_id = tg --token $alice.token user get $alice.user.id | from json
assert ($by_id.id == $alice.user.id) "getting a user by id should return that user"
assert (($by_id | get --optional tokens.local) != null) "getting a user by id should return a token"

let by_specifier = tg --token $alice.token user get alice | from json
assert ($by_specifier.id == $alice.user.id) "getting a user by specifier should return that user"
assert (($by_specifier | get --optional tokens.local) != null) "getting a user by specifier should return a token"
