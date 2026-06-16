use ../../test.nu *

# Adding a member that is already in the organization fails because the membership already exists.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

tg --token $alice.token organization create acme
tg --token $alice.token organization members add acme $bob.user.id

let output = tg --token $alice.token organization members add acme $bob.user.id | complete
failure $output "adding a member that already exists should fail"
assert ($output.stderr | str contains "already") "the error should report that the membership already exists"
