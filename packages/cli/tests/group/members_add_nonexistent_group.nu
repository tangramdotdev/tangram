use ../../test.nu *

# Adding a member to a group that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group members add ghost $alice.user.id | complete
failure $output "adding a member to a nonexistent group should fail"
assert ($output.stderr | str contains "failed to find the group") "the error should mention that the group was not found"
