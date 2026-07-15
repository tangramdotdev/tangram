use ../../test.nu *

# Logging in twice as the same user returns the same user, and each login issues a fresh token.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let first = tg login --verbose alice | from json
let second = tg login --verbose alice | from json

assert ($first.user.id == $second.user.id) "logging in again should return the same user"
assert ($first.token != $second.token) "each login should issue a new token"
