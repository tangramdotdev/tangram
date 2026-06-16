use ../../test.nu *

# Logging in as a new user creates the user, returns its record, and authenticates subsequent requests.

let server = spawn --config { authentication: true }

let user = tg user login alice | from json
assert ($user.id | str starts-with "usr_") "login should return a user id"
assert ($user.name == "alice") "the user name should match the login specifier"
assert ($user.emails | is-empty) "a user created without an email should have no emails"

# The token is written to the config, so whoami resolves to the same user.
let current = tg user whoami | from json
assert ($current.id == $user.id) "whoami should return the logged-in user"
