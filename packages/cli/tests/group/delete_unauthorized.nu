use ../../test.nu *

# Membership confers write but not admin, so a member cannot delete the group.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id

# Bob's membership grants him write, so he can create a subgroup under the team.
tg --token $bob.token group create team/bob-sub

# Write does not confer admin, so bob cannot delete the group.
let output = tg --token $bob.token group delete team | complete
failure $output "a member without admin should not be able to delete the group"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
