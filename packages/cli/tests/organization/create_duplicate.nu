use ../../test.nu *

# Creating an organization whose specifier is already in use fails with a clear error.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token organization create acme

let output = tg --token $alice.token organization create acme | complete
failure $output "a duplicate organization should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
