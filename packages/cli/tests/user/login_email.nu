use ../../test.nu *

# Logging in with an email associates the email with the user, and re-logging in with the same email is idempotent.

let server = spawn --config { authentication: true }

let user = tg user login alice --email alice@example.com | from json
assert ($user.emails == ["alice@example.com"]) "the email should be associated with the user"

# Re-logging in with the same email returns the same user without duplicating the email.
let again = tg user login alice --email alice@example.com | from json
assert ($again.id == $user.id) "re-login should return the same user"
assert ($again.emails == ["alice@example.com"]) "the email should not be duplicated"
