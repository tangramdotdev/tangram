use ../../test.nu *

# An email is globally unique, so a second user cannot claim an email already in use.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

tg login --name alice --email shared@example.com

let output = tg login --name bob --email shared@example.com | complete
failure $output "a second user should not be able to claim an email already in use"
