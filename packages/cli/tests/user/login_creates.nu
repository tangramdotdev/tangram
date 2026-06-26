use ../../test.nu *

# Logging in as a new user creates the user, returns its record, and authenticates subsequent requests.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
assert ($alice.user.id | str starts-with "usr_") "login should return a user id"
assert ($alice.user.name == "alice") "the user name should match the login specifier"
assert ($alice.user.emails | is-empty) "a user created without an email should have no emails"

# The token is written to the config, so whoami resolves to the same user.
let current = tg user whoami | from json
assert ($current.id == $alice.user.id) "whoami should return the logged-in user"
