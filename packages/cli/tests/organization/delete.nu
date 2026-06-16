use ../../test.nu *

# Deleting an organization removes it.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization delete acme

let output = tg --token $alice.token organization get acme | complete
failure $output "the organization should not exist after deletion"
assert ($output.stderr | str contains "failed to find the organization") "the deleted organization should no longer be found"
