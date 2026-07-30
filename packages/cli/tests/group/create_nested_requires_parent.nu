use ../../test.nu *

# A nested group requires its parent to exist.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token group create org/team/squad | complete
failure $output "creating a nested group should fail when its parent does not exist"

tg --token $alice.token group create org
tg --token $alice.token group create org/team
tg --token $alice.token group create org/team/squad
let leaf = tg --token $alice.token group get org/team/squad | from json
assert ($leaf.specifier == "org/team/squad") "the leaf group should have the full specifier"
