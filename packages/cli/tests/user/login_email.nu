use ../../test.nu *

# Logging in with an email associates the email with the user, and re-logging in with the same email is idempotent.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice --email alice@example.com | from json
assert ($alice.user.emails == ["alice@example.com"]) "the email should be associated with the user"

# Re-logging in with the same email returns the same user without duplicating the email.
let again = tg login --verbose alice --email alice@example.com | from json
assert ($again.user.id == $alice.user.id) "re-login should return the same user"
assert ($again.user.emails == ["alice@example.com"]) "the email should not be duplicated"
