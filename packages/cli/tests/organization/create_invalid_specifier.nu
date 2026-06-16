use ../../test.nu *

# An organization is flat, so a multi-component specifier is rejected.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization create acme/sub | complete
failure $output "a multi-component organization specifier should be rejected"
assert ($output.stderr | str contains "invalid organization specifier") "the error should mention an invalid organization specifier"
