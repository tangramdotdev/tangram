use ../lib/test.nu *

# Usage selects local independently of account management.
let remote = server spawn --name remote --config { usage: true }
let remote_group = tg group create foo | from json
let local = server spawn --name local --config {
	authentication: { users: { providers: { insecure: true } } },
	remotes: { default: { url: $remote.url } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
tg remote put default $remote.url
let group = tg group create --verbose foo | from json
let usage = tg usage | from json
open $local.config_path | upsert accounts false | to json | save --force $local.config_path
let local = server restart $local

assert equal (tg usage | from json) $usage
assert equal (tg user usage | from json) $usage
assert equal (tg usage alice | from json) $usage
assert equal (tg whoami --local | from json).id $alice.user.id
assert equal (tg group get foo | from json).id $remote_group.id
assert equal (tg group get --local foo | from json).id $group.data.id
assert equal (tg group get --local --remote foo | from json).id $group.data.id
assert equal (tg group get --local --remote $remote_group.id | from json).id $remote_group.id
assert equal (tg get foo | from json).id $group.data.id
let token = $group.tokens.local.0 | url encode --all
let reference = $"($group.data.id)?tokens[local][0]=($token)"
assert equal (tg get $reference | from json).id $group.data.id
failure (tg whoami | complete)
let output = tg login --local --name alice | complete
failure $output
assert ($output.stderr | str contains 'local account management is disabled')
