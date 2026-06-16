use ../../test.nu *

# A group with children cannot be deleted.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create parent/child

let output = tg --token $alice.token group delete parent | complete
failure $output "a group with children should not be deletable"
assert ($output.stderr | str contains "cannot delete a group with children") "the error should mention that the group has children"
