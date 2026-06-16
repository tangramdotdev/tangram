use ../../test.nu *

# A public read grant makes a resource readable by an anonymous client.

let server = spawn --config { authentication: true }

def current_token [] {
	open $env.TANGRAM_CONFIG | get token
}

tg user login alice
let alice = current_token

tg --token $alice group create team

# Before the public grant, an anonymous client cannot see the group.
let config = mktemp
{} | to json | save -f $config
let output = with-env { TANGRAM_CONFIG: $config } { tg group get team | complete }
failure $output "an anonymous client should not see a private group"

# After a public read grant, an anonymous client can read it.
tg --token $alice grant public read team
let output = with-env { TANGRAM_CONFIG: $config } { tg group get team | complete }
success $output "an anonymous client should be able to read a publicly granted group"
