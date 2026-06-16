use ../../test.nu *

# Adding a member that does not exist fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create team

# Create a group, capture its id, then delete it to obtain a valid but absent id.
let gone = tg --token $alice.token group create disposable | from json
tg --token $alice.token group delete disposable

let output = tg --token $alice.token group members add team $gone.id | complete
failure $output "adding a nonexistent member should fail"
assert ($output.stderr | str contains "failed to find the member") "the error should mention that the member was not found"
