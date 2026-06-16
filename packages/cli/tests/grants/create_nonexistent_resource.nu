use ../../test.nu *

# Granting on a resource that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let output = tg --token $alice.token grant $bob.user.id read ghostgroup | complete
failure $output "granting on a nonexistent resource should fail"
assert ($output.stderr | str contains "failed to find the resource") "the error should mention that the resource was not found"
