use ../../test.nu *

# Getting a group that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group get ghost | complete
failure $output "getting a nonexistent group should fail"
assert ($output.stderr | str contains "failed to find the group") "the error should mention that the group was not found"
