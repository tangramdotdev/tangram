use ../../test.nu *

# Revoking the write grant on the owning group removes a non-member's access to the group-owned sandbox.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim

tg --token $alice.token grant $carol.user.id write team
success (tg --token $carol.token sandbox get $sandbox | complete) "Carol should get the sandbox while she has write on the team"

# Carol is not a member, so revoking her write grant leaves no access path.
tg --token $alice.token revoke $carol.user.id write team
failure (tg --token $carol.token sandbox get $sandbox | complete) "Carol must not get the sandbox after her write grant is revoked"
let carol_list = tg --token $carol.token sandbox list | from json
assert ($carol_list | where id == $sandbox | is-empty) "Carol must not see the sandbox after her write grant is revoked"
