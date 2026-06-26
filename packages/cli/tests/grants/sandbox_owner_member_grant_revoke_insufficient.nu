use ../../test.nu *

# Revoking a member's write grant does not remove their access while their membership remains, because membership is evaluated dynamically.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token group members add team $bob.user.id
let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should get the team-owned sandbox while a member"

# Deleting the write grant created when Bob joined leaves his membership intact, so dynamic membership still authorizes him.
tg --token $alice.token revoke $bob.user.id write team
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should still get the sandbox while a member, even after the write grant is revoked"
