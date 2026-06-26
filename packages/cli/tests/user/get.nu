use ../../test.nu *

# A user can get their own record by id and by specifier.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json

let by_id = tg --token $alice.token user get $alice.user.id | from json
assert ($by_id.id == $alice.user.id) "getting a user by id should return that user"

let by_specifier = tg --token $alice.token user get alice | from json
assert ($by_specifier.id == $alice.user.id) "getting a user by specifier should return that user"
