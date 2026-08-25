use ../../test.nu *

# A nested group requires its parent by default, while -p creates missing parent groups.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let output = tg --token $alice.token group create org/team/squad | complete
failure $output "creating a nested group should fail when its parent does not exist"

let created = tg --token $alice.token group create -p org/team/squad | from json
let leaf = tg --token $alice.token group get org/team/squad | from json
assert ($leaf.specifier == "org/team/squad") "the leaf group should have the full specifier"
assert equal $leaf.id $created.id

let existing = tg --token $alice.token group create -p org/team/squad | from json
assert equal $existing.id $created.id
