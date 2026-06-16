use ../../test.nu *

# Creating a grant that already exists fails.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id read team

let output = tg --token $alice.token grant $bob.user.id read team | complete
failure $output "creating a grant that already exists should fail"
assert ($output.stderr | str contains "already") "the error should report that the grant already exists"
