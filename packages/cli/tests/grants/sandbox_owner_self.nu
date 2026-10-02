use ../lib/test.nu *

# A user may explicitly name themselves as the owner of a sandbox they create.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let sandbox = tg --token $alice.token sandbox create --no-tokens --owner $alice.user.id --no-network | referent node
let data = tg --token $alice.token sandbox get $sandbox | from json | get data
assert equal $data.owner $alice.user.id "a user should be able to name themselves as the owner"

tg --token $alice.token sandbox destroy $sandbox
