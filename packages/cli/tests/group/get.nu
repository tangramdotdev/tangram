use ../../test.nu *

# A group can be retrieved by its id and by its specifier.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let group = tg --token $alice.token group create project | from json

let by_id = tg --token $alice.token group get $group.id | from json
assert ($by_id.id == $group.id) "getting a group by id should return that group"

let by_specifier = tg --token $alice.token group get project | from json
assert ($by_specifier.id == $group.id) "getting a group by specifier should return that group"
