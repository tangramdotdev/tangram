use ../../test.nu *

# whoami returns the logged-in user and reports not-logged-in for an anonymous client.

let server = spawn --config { authentication: true }

let user = tg user login alice | from json
let me = tg user whoami | from json
assert ($me.id == $user.id) "whoami should return the logged-in user"

# An anonymous client has no current user.
let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg user whoami | complete }
failure $output "an anonymous client should not be logged in"
assert ($output.stderr | str contains "not logged in") "the error should mention not being logged in"
