use ../../test.nu *

# Creating a nested group auto-creates its ancestor groups.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

tg --token $alice.token group create org/team/squad

# Each ancestor exists.
tg --token $alice.token group get org
tg --token $alice.token group get org/team
let leaf = tg --token $alice.token group get org/team/squad | from json
assert ($leaf.specifier == "org/team/squad") "the leaf group should have the full specifier"
