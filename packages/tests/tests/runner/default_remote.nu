use ../../util/test.nu *

# Runner management defaults depend on runners rather than accounts or execution configuration.
let remote = server spawn --name remote --config { runners: true }
let local = server spawn --name local --config {
	accounts: true,
	runners: false,
	remotes: {
		default: { url: $remote.url },
		staging: { url: $remote.url },
	},
}
let runner = tg runner create | from json
assert equal (tg runner list --all | from json | get node) [$runner.data.id]
assert equal (tg runner list --local | from json) []
let runners = tg runner list -r=staging --all | from json
assert equal ($runners | get node) [$runner.data.id]
assert equal ($runners | get options.location) [remote:staging]
assert equal (tg runner list -r=staging --all --expand | from json | get location) [remote:staging]
let output = tg --no-quiet runner get $"($runner.data.id)?location=remote:staging" | complete
success $output
assert equal ($output.stdout | from json | get id) $runner.data.id
assert ($output.stderr | str contains "remote:staging") "getting a listed runner should report its location"
let token = tg runner token create $runner.data.id | from json
assert ((tg runner token list --all $runner.data.id | from json | get node) | any { $in == $token.id })
let listed = tg runner token list -r=staging --all $runner.data.id | from json | where node == $token.id | first
tg runner token delete $runner.data.id $"($listed.node)?location=($listed.options.location)"
assert not ((tg runner token list --all $runner.data.id | from json | get node) | any { $in == $token.id })
let output = tg runner create --local | complete
failure $output
assert ($output.stderr | str contains 'local runner management is disabled')
let path = artifact { tangram.ts: 'export default () => "hello";' }
assert equal (tg build $path | from json) hello
tg runner delete $runner.data.id
assert equal (tg runner list | from json) []

let local = server spawn --name enabled --config {
	accounts: false,
	runners: true,
	remotes: { default: { url: $remote.url } },
}
let runner = tg runner create | from json
assert equal (tg runner list | from json | get node) [$runner.data.id]
assert equal (tg runner list --remote | from json) []
tg runner delete $runner.data.id
