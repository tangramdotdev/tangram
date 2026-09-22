use ../lib/test.nu *

# Only runners may obtain new identities on deferred control connections.
let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let socket = $remote.url | str replace 'http+unix://' '' | url decode
for entry in [
	{ path: 'processes/control?start=false', content_type: 'application/vnd.tangram.process-control' }
	{ path: 'sandboxes/control?create=false', content_type: 'text/event-stream' }
] {
	let headers = { Authorization: $'Bearer ($root_token)', 'Content-Type': $entry.content_type }
	let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/($entry.path)' ''
	assert equal $output.status 500
	assert ($output.body | to text | str contains 'requires a runner') ($output.body | to text)
}
