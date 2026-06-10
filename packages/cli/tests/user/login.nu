use ../../test.nu *

# Logging in associates emails with a user, is idempotent across repeated logins, and rejects an email that is already in use by another user.

let server = spawn --config { authentication: true }

let alice = tg login alice | from json
assert equal $alice.specifier alice
assert equal $alice.emails []
let server_config_columns = open $env.TANGRAM_CONFIG | columns | where $it not-in [token tracing]
assert equal $server_config_columns []

let alice = tg login alice --email alice@example.com | from json
assert equal $alice.specifier alice
assert equal $alice.emails [alice@example.com]

let alice = tg login alice | from json
assert equal $alice.specifier alice
assert equal $alice.emails [alice@example.com]

let output = tg login bob --email alice@example.com | complete
failure $output "Another user should not be able to reuse an email."
