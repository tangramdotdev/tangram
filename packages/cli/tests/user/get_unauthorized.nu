use ../../test.nu *

# A user without read permission cannot get another user's record.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let output = tg --token $eve.token user get $alice.user.id | complete
failure $output "a user without read permission should not be able to get another user"
assert ($output.stderr | str contains "failed to find the user") "the user should not be visible without read permission"
