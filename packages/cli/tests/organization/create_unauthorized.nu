use ../../test.nu *

# An anonymous client cannot create an organization.

let server = spawn --config { authentication: true }

let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg organization create anon | complete }
failure $output "an anonymous client should not be able to create an organization"
assert ($output.stderr | str contains "unauthorized") "the error should mention that the request is unauthorized"
