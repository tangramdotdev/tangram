use ../lib/test.nu *

# Only runners may obtain new sandbox identities on control connections waiting for create.
let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': 'text/event-stream' }
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers 'http://localhost/sandboxes/control' ''
assert equal $output.status 500
assert ($output.body | to text | str contains 'requires a runner') ($output.body | to text)
