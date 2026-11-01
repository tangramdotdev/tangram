use ../../util/test.nu *

# A process spawned for an organization records the organization as its owner, and members of the organization see it when listing by owner, while outsiders do not.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
let acme = tg --token $alice.token organization get acme | from json

let path = artifact {
	tangram.ts: 'export default () => "done";',
}
let output = tg --token $alice.token build --detach --verbose --organization acme $path | from json
let process = $output.process | split row '?' | first
tg --token $alice.token process wait $process | ignore
tg index

let data = tg --token $alice.token process get $process | from json
assert equal $data.creator $alice.user.id "the process creator should be the user who spawned it"
assert equal $data.owner $acme.id "the process owner should be the organization"

# Alice sees the process she created, and Bob does not see it by creator.
let alice_list = tg --token $alice.token ps --any-status | from json
assert (($alice_list | where node == $process | is-empty) == false) "Alice should see the process she created"
let bob_list = tg --token $bob.token ps --any-status | from json
assert ($bob_list | where node == $process | is-empty) "Bob must not see Alice's process by creator"

# Listing by the organization includes the process for its members, and not for Eve, who is not a member.
let alice_owned = tg --token $alice.token ps --any-status --owner $acme.id | from json
assert (($alice_owned | where node == $process | is-empty) == false) "Alice should see her process when listing by the organization"
let bob_owned = tg --token $bob.token ps --any-status --owner $acme.id | from json
assert (($bob_owned | where node == $process | is-empty) == false) "Bob should see a process owned by his organization"
let eve_owned = tg --token $eve.token ps --any-status --owner $acme.id | from json
assert ($eve_owned | where node == $process | is-empty) "Eve must not see a process owned by an organization she is not in"

# Listing by owner and creator together narrows to the creator.
let alice_created = tg --token $alice.token ps --any-status --owner $acme.id --creator $bob.user.id | from json
assert ($alice_created | where node == $process | is-empty) "Alice must not see her process when filtering by another creator"
