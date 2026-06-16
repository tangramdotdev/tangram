use ../../test.nu *

# An organization with children cannot be deleted.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token organization create acme

# Tagging under the organization gives it a child.
let id = tg --token $alice.token checkin (artifact 'x')
tg --token $alice.token tag acme/foo $id

let output = tg --token $alice.token organization delete acme | complete
failure $output "an organization with children should not be deletable"
assert ($output.stderr | str contains "cannot delete an organization with children") "the error should mention that the organization has children"
