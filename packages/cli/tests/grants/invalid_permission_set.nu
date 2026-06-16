use ../../test.nu *

# A malformed permission set is rejected before it reaches the server.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team

# Mixing permission kinds in one set is rejected.
let mixed = tg --token $alice.token grant $bob.user.id read,object_node team | complete
failure $mixed "mixing permission kinds in a set should be rejected"
assert ($mixed.stderr | str contains "invalid grant permissions") "the error should report that the permission set is invalid"

# An unknown permission token is rejected.
let unknown = tg --token $alice.token grant $bob.user.id frobnicate team | complete
failure $unknown "an unknown permission should be rejected"
assert ($unknown.stderr | str contains "invalid grant permission") "the error should report that the permission is invalid"
