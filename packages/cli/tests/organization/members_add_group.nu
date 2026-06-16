use ../../test.nu *

# A group can be added as a member of an organization.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token organization create acme
let team = tg --token $alice.token group create team | from json
tg --token $alice.token organization members add acme $team.id

let members = tg --token $alice.token organization members list acme | from json
assert ($team.id in $members) "the added group should be listed as a member"
