use ../../test.nu *

# A group can be added as a member of another group.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create team
let other = tg --token $alice.token group create other | from json
tg --token $alice.token group members add team $other.id

let members = tg --token $alice.token group members list team | from json
assert ($other.id in $members) "the added group should be listed as a member"
