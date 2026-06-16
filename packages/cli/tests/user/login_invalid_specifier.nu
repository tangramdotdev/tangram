use ../../test.nu *

# A user specifier must be a single component, so a multi-component login is rejected.

let server = spawn --config { authentication: true }

let output = tg user login "alice/bob" | complete
failure $output "a multi-component user specifier should be rejected"
assert ($output.stderr | str contains "invalid user specifier") "the error should mention an invalid user specifier"
