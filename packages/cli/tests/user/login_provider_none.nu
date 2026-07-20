use ../../test.nu *

# Login fails when authentication is enabled but no authentication providers are configured.

let server = spawn --config { authentication: { users: true } }

let output = tg login alice | complete
failure $output "login should fail without a configured authentication provider"
assert ($output.stderr | str contains "no authentication providers are configured") "the error should explain that no authentication providers are configured"
