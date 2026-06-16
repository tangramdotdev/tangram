use ../../test.nu *

# An admin can remove a member from an organization.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $alice.token organization members remove acme $bob.user.id

let members = tg --token $alice.token organization members list acme | from json
assert (not ($bob.user.id in $members)) "the removed user should no longer be a member"
