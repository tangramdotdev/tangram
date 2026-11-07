use ../../util/test.nu *

# An inherited token applies only to the inherited server, so naming a server on the command line does not send it there.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let config = mktemp
{} | to json | save -f $config

# The inherited token applies when the command line does not name a server.
let output = with-env { TANGRAM_CONFIG: $config, TANGRAM_TOKEN: $alice.token } { tg user whoami | complete }
success $output "the inherited token should authenticate with the inherited server"

# The inherited token does not apply when the command line names the server with a URL.
let output = with-env { TANGRAM_CONFIG: $config, TANGRAM_TOKEN: $alice.token } { tg --url $remote.url user whoami | complete }
failure $output "the inherited token should not apply to a server named on the command line"

# An explicit token still applies to a named server.
let output = with-env { TANGRAM_CONFIG: $config } { tg --url $remote.url --token $alice.token user whoami | complete }
success $output "an explicit token should authenticate with a named server"
