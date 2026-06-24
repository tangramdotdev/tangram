use ../../test.nu *

# An organization member loses get, list, and destroy on an organization-owned sandbox once their membership is revoked.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id

let acme = tg --token $bob.token organization get acme | from json
let sandbox = tg --token $bob.token sandbox create --owner acme --no-network | str trim

let data = tg --token $bob.token sandbox get $sandbox | from json
assert equal $data.owner $acme.id "the sandbox should be owned by the organization"

# While Bob is a member, he can get, list, and (later) destroy the organization-owned sandbox, and Eve cannot.
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should get a sandbox owned by his organization"
let bob_list = tg --token $bob.token sandbox list | from json
assert (($bob_list | where id == $sandbox | is-empty) == false) "Bob should see the organization-owned sandbox while a member"
failure (tg --token $eve.token sandbox get $sandbox | complete) "Eve must not get a sandbox owned by an organization she is not in"

# Revoking Bob's membership removes both his membership and the auto-granted write on the organization.
tg --token $alice.token organization members remove acme $bob.user.id

failure (tg --token $bob.token sandbox get $sandbox | complete) "Bob must not get the sandbox after his membership is revoked"
let bob_list_after = tg --token $bob.token sandbox list | from json
assert ($bob_list_after | where id == $sandbox | is-empty) "Bob must not see the sandbox after his membership is revoked"
failure (tg --token $bob.token sandbox destroy $sandbox | complete) "Bob must not destroy the sandbox after his membership is revoked"

# Alice administers the organization, so her access is independent of Bob's membership.
success (tg --token $alice.token sandbox get $sandbox | complete) "Alice should still get the sandbox she administers via the organization"
