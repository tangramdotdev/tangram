use ../../test.nu *

# whoami returns the logged-in user and reports not-logged-in for an anonymous client.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let me = tg user whoami | from json
assert ($me.id == $alice.user.id) "whoami should return the logged-in user"

# An anonymous client has no current user.
let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg user whoami | complete }
failure $output "an anonymous client should not be logged in"
snapshot --normalize $output.stderr '
	error an error occurred
	-> not logged in

'

# A named-remote login persists its token for subsequent commands at that location.
let remote = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let local = server spawn --config {
	remotes: { staging: { url: $remote.url } }
}

let bob = tg login -r=staging --name bob | from json
let staging = tg remote get staging | from json
assert (($staging | get --optional token) != null) "login -r=staging should save the token"

let remote_user = tg whoami -r=staging | from json
assert ($remote_user.id == $bob.id) "whoami -r=staging should return the remote user"

let output = tg whoami --local | complete
failure $output "whoami --local should not return a remote user"
snapshot --normalize $output.stderr '
	error an error occurred
	-> not logged in

'
