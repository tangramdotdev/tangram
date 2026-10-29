use ../../lib/test.nu *

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
assert equal (tg runner list --all | from json | get id) [$runner.data.id]
assert equal (tg runner list --local | from json) []
assert equal (tg runner list -r=staging --all | from json | get id) [$runner.data.id]
let token = tg runner token create $runner.data.id | from json
assert ((tg runner token list --all $runner.data.id | from json | get id) | any { $in == $token.id })
tg runner token delete $runner.data.id $token.id
assert not ((tg runner token list --all $runner.data.id | from json | get id) | any { $in == $token.id })
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
assert equal (tg runner list | from json | get id) [$runner.data.id]
assert equal (tg runner list --remote | from json) []
tg runner delete $runner.data.id
