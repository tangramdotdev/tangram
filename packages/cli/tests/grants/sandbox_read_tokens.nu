use ../lib/test.nu *

# Sandbox read capabilities authorize get and status, including explicit sources.
let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	runner: { process_state_ttl: 60, sandbox_state_ttl: 60 },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let sandbox = tg --url $server.url --token $alice.token sandbox create --no-network | str trim
let other = tg --url $server.url --token $alice.token sandbox create --no-network | str trim
let path = artifact { tangram.ts: 'export default () => tg.file("sandbox-read-token");' }
let process = tg --url $server.url --token $alice.token spawn $'--sandbox=($sandbox)' $path | str trim | split row '?' | first
tg --url $server.url --token $alice.token wait --source=index $process | ignore
tg --url $server.url --token $alice.token index
let socket = $server.url | str replace 'http+unix://' '' | url decode
let output = tg --url $server.url --token $alice.token sandbox get $sandbox | from json
let token = $output.tokens.local.0
let query = $'tokens[local][0]=($token | url encode --all)'
let reference = $'($sandbox)?($query)'
for source in [auto runner index] {
	failure (tg --url $server.url --token $bob.token sandbox get $'--source=($source)' $sandbox | complete)
	success (tg --url $server.url --token $bob.token sandbox get $'--source=($source)' $reference | complete)
	# The sandbox capability permits listing IDs but does not authorize reading those processes.
	if $source != index {
		let listing = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/processes?source=($source)&timeout=0&($query)'
		let processes = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json } | get data | flatten
		assert equal $processes [$process]
	}
	failure (tg --url $server.url --token $bob.token process get $'--source=($source)' $'($process)?($query)' | complete)
	let status = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/status?source=($source)&timeout=0&($query)'
	assert ($status | str contains 'started')
	let denied = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($other)/status?source=($source)&timeout=0&($query)'
	assert equal $denied.status 404
}
# Remote forwarding keeps the capability scoped to its issuer.
let client = server spawn --name client --config { remotes: { default: { url: $server.url } } }
let client_socket = $client.url | str replace 'http+unix://' '' | url decode
let remote_query = $'location=remote&tokens[remote][0]=($token | url encode --all)'
for source in [auto runner index] {
	let remote = http get --max-time 10sec --unix-socket $client_socket $'http://localhost/sandboxes/($sandbox)?source=($source)&($remote_query)'
	assert equal $remote.data.id $sandbox
	let status = http get --raw --max-time 10sec --unix-socket $client_socket $'http://localhost/sandboxes/($sandbox)/status?source=($source)&timeout=0&($remote_query)'
	assert ($status | str contains 'started')
}
tg --url $server.url --token $alice.token sandbox destroy $sandbox
tg --url $server.url --token $alice.token sandbox wait --source=index $sandbox | ignore
success (tg --url $server.url --token $bob.token sandbox wait --source=index $reference | complete)
let listing = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/processes?source=index&($query)'
let processes = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json } | get data | flatten
assert equal $processes [$process]

tg --url $server.url --token $alice.token sandbox destroy $other
