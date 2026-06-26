use ../../test.nu *

# An admin can remove a member from a group.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
tg --token $alice.token group members remove team $bob.user.id

let members = tg --token $alice.token group members list team | from json
assert (not ($bob.user.id in $members)) "the removed user should no longer be a member"
