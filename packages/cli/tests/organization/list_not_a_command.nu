use ../../test.nu *

# Entities are listed with tg list by specifier, so there is no tg organization list subcommand, just as there is none for groups.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token organization list | complete
failure $output "tg organization list should not be a command"
assert ($output.stderr | str contains "unrecognized subcommand") "organization list should be rejected as an unknown subcommand"
