use ../../test.nu *

# Adding a member to an organization that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization members add ghost $alice.user.id | complete
failure $output "adding a member to a nonexistent organization should fail"
assert ($output.stderr | str contains "failed to find the organization") "the error should mention that the organization was not found"
