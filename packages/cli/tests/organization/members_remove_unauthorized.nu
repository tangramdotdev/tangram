use ../../test.nu *

# Membership confers write but not admin, so a member cannot remove another member.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id
tg --token $alice.token organization members add acme $carol.user.id

# Bob's membership grants him write but not admin, so he cannot remove another member.
let output = tg --token $bob.token organization members remove acme $carol.user.id | complete
failure $output "a member without admin should not be able to remove another member"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
