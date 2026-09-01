use ../../test.nu *

# A sandbox owner can be provided as a specifier, and write on that owner grants sandbox access.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let eve = tg login --verbose --name eve | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team

let team = tg --token $bob.token group get team | from json
let sandbox = tg --token $bob.token sandbox create --owner team --no-network | str trim
let data = tg --token $bob.token sandbox get $sandbox | from json | get data

assert equal $data.owner $team.id "the sandbox should store the resolved owner"
success (tg --token $alice.token sandbox get $sandbox | complete) "Alice should get a sandbox owned by her group"
success (tg --token $bob.token sandbox get $sandbox | complete) "Bob should get a sandbox owned by a group he can write"
failure (tg --token $eve.token sandbox get $sandbox | complete) "Eve must not get a sandbox owned by a group she cannot access"

tg --token $bob.token sandbox destroy $sandbox
