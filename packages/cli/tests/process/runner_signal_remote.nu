use ../../test.nu *

# Reading local runner state must not redirect signals away from the remote process control connection.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: 'export default async () => { console.log("ready"); await tg.sleep(120); };',
}
let spawned = tg --url $local.url spawn --remote --network --verbose $path | from json
let process = $spawned.process
let log = timeout 30s tg --url $local.url process log --no-timeout --length 6 $process | str trim
assert equal $log ready

let socket = $runner.url | str replace 'http+unix://' '' | url decode
let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let remote_response = http get --unix-socket $remote_socket --headers { Authorization: $'Bearer ($root_token)' } $'http://localhost/processes/($process)'
let query = { location: remote, 'tokens[remote][authorization][0]': $remote_response.tokens.local.authorization.0 } | url build-query
let response = http get --unix-socket $socket $'http://localhost/processes/($process)?($query)'
assert equal $response.location remote
assert equal $response.data.status started

# An explicit local request must not signal the remote process.
let body = { location: local, signal: TERM, tokens: $spawned.tokens } | to json --raw
let status = (
	$body | into binary | http post --allow-errors --headers { 'Content-Type': 'application/json' }
		--unix-socket $socket $'http://localhost/processes/($process)/signal'
	| metadata | get http_response.status
)
assert equal $status 404

# Without a location, signaling skips the remote index entry and uses normal remote dispatch.
# Signaling requires the parent capability from spawn, not the node capability from get.
let body = { signal: TERM, tokens: $spawned.tokens } | to json --raw
let status = (
	$body | into binary | http post --allow-errors --headers { 'Content-Type': 'application/json' }
		--unix-socket $socket $'http://localhost/processes/($process)/signal'
	| metadata | get http_response.status
)
assert equal $status 200 "the process location returned by get must route signals through remote control"

let output = timeout 30s tg --url $local.url wait $process | from json
assert equal $output.exit 143
