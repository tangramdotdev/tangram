use ../../test.nu *

# A user cannot log in with a specifier already claimed by a group.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
tg --token $alice.token group create shared

let output = tg login shared | complete
failure $output "logging in with a specifier claimed by a group should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
