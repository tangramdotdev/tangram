use ../../util/test.nu *

# A member who did not create a group-owned sandbox loses get, list, and destroy on it once their membership is revoked, while its creator keeps access.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members add team $carol.user.id

let team = tg --token $bob.token group get team | from json
let sandbox = tg --token $bob.token sandbox create --no-tokens --group team --no-network | referent node

let data = tg --token $bob.token sandbox get $sandbox | from json | get data
assert equal $data.owner $team.id "the sandbox should be owned by the team"

# While Carol is a member, she can get, list, and (later) destroy the team-owned sandbox, and Eve cannot.
success (tg --token $carol.token sandbox get $sandbox | complete) "Carol should get a sandbox owned by her group"
let carol_list = tg --token $carol.token sandbox list --owner $team.id | from json
assert (($carol_list | where node == $sandbox | is-empty) == false) "Carol should see the team-owned sandbox while a member"
failure (tg --token $eve.token sandbox get $sandbox | complete) "Eve must not get a sandbox owned by a group she is not in"

# Revoking Carol's and Bob's memberships removes their memberships and the auto-granted write on the team.
tg --token $alice.token group members remove team $carol.user.id
tg --token $alice.token group members remove team $bob.user.id
tg index

failure (tg --token $carol.token sandbox get $sandbox | complete) "Carol must not get the sandbox after her membership is revoked"
let carol_list_after = tg --token $carol.token sandbox list --owner $team.id | from json
assert ($carol_list_after | where node == $sandbox | is-empty) "Carol must not see the sandbox after her membership is revoked"
failure (tg --token $carol.token sandbox destroy $sandbox | complete) "Carol must not destroy the sandbox after her membership is revoked"

# Bob created the sandbox, so he keeps access after his membership is revoked.
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should still get the sandbox he created"

# Alice administers the team, so her access is independent of Carol's membership.
success (tg --token $alice.token sandbox get $sandbox | complete) "Alice should still get the sandbox she administers via the team"

tg --token $alice.token sandbox destroy $sandbox
