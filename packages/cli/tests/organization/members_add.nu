use ../../test.nu *

# An admin can add a member to an organization, and the member is listed.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id

let members = tg --token $alice.token organization members list acme | from json
assert ($bob.user.id in $members) "the added user should be listed as a member"
