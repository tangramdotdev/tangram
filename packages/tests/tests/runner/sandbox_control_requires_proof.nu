use ../../lib/test.nu *

# Only a runner that proves that it derived a sandbox's ID may open a sandbox control connection for a new sandbox and receive its token.

const helper = path self '../../lib/sandbox_control_connect.py'
let root_token = random chars
let local = server spawn --config { authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } } }
let mallory = tg login --verbose --name mallory | from json
let socket = $local.url | str replace 'http+unix://' '' | url decode
let runner = tg --token $mallory.token runner create --owner $mallory.user.id | from json
let other = tg --token $mallory.token runner create --owner $mallory.user.id | from json

# A runner without a proof, and the root, are rejected.
{ create: {}, location: 'local', runner: $runner.data.id } | to json | save --force arg.json
for token in [$runner.token.token $root_token] {
	let output = python3 $helper $socket $token arg.json | complete
	failure $output "a connection for a new sandbox should require a proof"
	assert ($output.stderr | str starts-with '403') $output.stderr
}

# A runner must not connect a sandbox whose ID another runner derived, or name another runner.
{ create: {}, deriver: $other.data.id, location: 'local', runner: $runner.data.id } | to json | save --force arg.json
let output = python3 $helper $socket $runner.token.token arg.json | complete
failure $output "a runner should not connect a sandbox whose ID another runner derived"
assert ($output.stderr | str contains 'did not derive') $output.stderr
{ create: {}, deriver: $other.data.id, location: 'local', runner: $other.data.id } | to json | save --force arg.json
let output = python3 $helper $socket $runner.token.token arg.json | complete
failure $output "a runner should not connect a sandbox for another runner"
assert ($output.stderr | str contains 'must be its principal') $output.stderr

# A runner that derived the sandbox's ID receives the sandbox's token.
{ create: {}, deriver: $runner.data.id, location: 'local', runner: $runner.data.id } | to json | save --force arg.json
let output = python3 $helper $socket $runner.token.token arg.json | complete
success $output "a runner should connect a sandbox whose ID it derived"
let header = $output.stdout | from json
assert ($header.token | is-not-empty) "the header should carry the sandbox's token"
