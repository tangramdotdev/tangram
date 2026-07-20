use ../../test.nu *

# A user inherently has permission over their own namespace, so they can tag and create groups directly under their username.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let path = artifact 'hello'
let id = tg --token $alice.token checkin $path

# Alice can publish a tag under her own username namespace.
tg --token $alice.token tag alice/foo $id
let tag = tg --token $alice.token tag get alice/foo | from json
assert ($tag.specifier == "alice/foo") "the tag should live under alice's namespace"
assert ($tag.parent == $alice.user.id) "the tag's parent should be alice's own user node"

# Alice can also create a group under her own namespace.
tg --token $alice.token group create alice/proj
