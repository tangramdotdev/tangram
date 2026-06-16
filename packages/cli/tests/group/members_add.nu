use ../../test.nu *

# An admin can add a user to a group; the member is listed and gains write on the group.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

let members = tg --token $alice.token group members list team | from json
assert ($bob.user.id in $members) "the added user should be listed as a member"

# Membership confers write, so the member can create a subgroup under the team.
tg --token $bob.token group create team/bob-project
