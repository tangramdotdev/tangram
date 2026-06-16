use ../../test.nu *

# A group cannot be created with a specifier that is already in use.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create project
let output = tg --token $alice.token group create project | complete
failure $output "creating a duplicate group should be rejected"
assert ($output.stderr | str contains "already in use") "the error should mention that the specifier is already in use"
