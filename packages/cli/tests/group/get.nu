use ../../test.nu *

# A group can be retrieved by its id and by its specifier.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let group = tg --token $alice.token group create project | from json

let by_id = tg --token $alice.token group get $group.id | from json
assert ($by_id.id == $group.id) "getting a group by id should return that group"
assert (($by_id | get --optional tokens.local) != null) "getting a group by id should return a token"

let by_specifier = tg --token $alice.token group get project | from json
assert ($by_specifier.id == $group.id) "getting a group by specifier should return that group"
assert (($by_specifier | get --optional tokens.local) != null) "getting a group by specifier should return a token"

let generic = tg --token $alice.token get $group.id | from json
assert ($generic.id == $group.id) "generic get should return the group"
assert (($generic | get --optional tokens.local) != null) "generic get should return a token"
