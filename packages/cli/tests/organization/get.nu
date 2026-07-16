use ../../test.nu *

# An organization can be retrieved by its id and by its specifier.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let organization = tg --token $alice.token organization create acme | from json

let by_id = tg --token $alice.token organization get $organization.id | from json
assert ($by_id.id == $organization.id) "getting an organization by id should return that organization"

let by_specifier = tg --token $alice.token organization get acme | from json
assert ($by_specifier.id == $organization.id) "getting an organization by specifier should return that organization"
