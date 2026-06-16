use ../../test.nu *

# An email is globally unique, so a second user cannot claim an email already in use.

let server = spawn --config { authentication: true }

tg login alice --email shared@example.com

let output = tg login bob --email shared@example.com | complete
failure $output "a second user should not be able to claim an email already in use"
