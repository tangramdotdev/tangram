use ../../test.nu *

# A group member loses get, list, and destroy on a group-owned sandbox once their membership is revoked.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

let team = tg --token $bob.token group get team | from json
let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim

let data = tg --token $bob.token sandbox get $sandbox | from json
assert equal $data.owner $team.id "the sandbox should be owned by the team"

# While Bob is a member, he can get, list, and (later) destroy the team-owned sandbox, and Eve cannot.
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should get a sandbox owned by his group"
let bob_list = tg --token $bob.token sandbox list | from json
assert (($bob_list | where id == $sandbox | is-empty) == false) "Bob should see the team-owned sandbox while a member"
failure (tg --token $eve.token sandbox get $sandbox | complete) "Eve must not get a sandbox owned by a group she is not in"

# Revoking Bob's membership removes both his membership and the auto-granted write on the team.
tg --token $alice.token group members remove team $bob.user.id

failure (tg --token $bob.token sandbox get $sandbox | complete) "Bob must not get the sandbox after his membership is revoked"
let bob_list_after = tg --token $bob.token sandbox list | from json
assert ($bob_list_after | where id == $sandbox | is-empty) "Bob must not see the sandbox after his membership is revoked"
failure (tg --token $bob.token sandbox destroy $sandbox | complete) "Bob must not destroy the sandbox after his membership is revoked"

# Alice administers the team, so her access is independent of Bob's membership.
success (tg --token $alice.token sandbox get $sandbox | complete) "Alice should still get the sandbox she administers via the team"
