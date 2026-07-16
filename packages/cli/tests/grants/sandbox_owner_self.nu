use ../../test.nu *

# A user may explicitly name themselves as the owner of a sandbox they create.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json

let sandbox = tg --token $alice.token sandbox create --owner $alice.user.id --no-network | str trim
let data = tg --token $alice.token sandbox get $sandbox | from json
assert equal $data.owner $alice.user.id "a user should be able to name themselves as the owner"
