use ../../util/test.nu *

# Sandbox read capabilities authorize get and status, including explicit sources.
let local = server spawn --name local --config {
	authentication: { users: { providers: { insecure: true } } },
	runner: { process_state_time_to_live: 60, sandbox_state_time_to_live: 60 },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let sandbox = tg --url $local.url --token $alice.token sandbox create --no-tokens --no-network | referent node
let other = tg --url $local.url --token $alice.token sandbox create --no-tokens --no-network | referent node
let path = artifact { tangram.ts: 'export default () => tg.file("sandbox-read-token");' }
let process = tg --url $local.url --token $alice.token spawn --no-tokens $'--sandbox=($sandbox)' $path | referent node | split row '?' | first
tg --url $local.url --token $alice.token wait $process | ignore
tg --url $local.url --token $alice.token index
let socket = $local.url | str replace 'http+unix://' '' | url decode
let output = tg --url $local.url --token $alice.token sandbox get $sandbox | from json
let token = $output.tokens.local.0
let query = $'tokens[local][0]=($token | url encode --all)'
let reference = $'($sandbox)?($query)'
failure (tg --url $local.url --token $bob.token sandbox get $sandbox | complete)
success (tg --url $local.url --token $bob.token sandbox get $reference | complete)
# The sandbox capability permits listing IDs but does not authorize reading those processes.
let listing = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/processes?timeout=0&($query)'
let processes = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json } | get data | flatten
assert equal $processes [$process]
failure (tg --url $local.url --token $bob.token process get $'($process)?($query)' | complete)
let status = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/status?timeout=0&($query)'
assert ($status | str contains 'started')
let denied = http get --allow-errors --full --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($other)/status?timeout=0&($query)'
assert equal $denied.status 404
# Remote forwarding keeps the capability scoped to its issuer.
let local_client = server spawn --name local-client --config { remotes: { default: { url: $local.url } } }
let local_client_socket = $local_client.url | str replace 'http+unix://' '' | url decode
let remote_query = $'location=remote&tokens[remote][0]=($token | url encode --all)'
let remote = http get --max-time 10sec --unix-socket $local_client_socket $'http://localhost/sandboxes/($sandbox)?($remote_query)'
assert equal $remote.data.id $sandbox
let status = http get --raw --max-time 10sec --unix-socket $local_client_socket $'http://localhost/sandboxes/($sandbox)/status?timeout=0&($remote_query)'
assert ($status | str contains 'started')
tg --url $local.url --token $alice.token sandbox destroy $sandbox
tg --url $local.url --token $alice.token sandbox wait $sandbox | ignore
success (tg --url $local.url --token $bob.token sandbox wait $reference | complete)
let listing = http get --raw --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/sandboxes/($sandbox)/processes?($query)'
let processes = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json } | get data | flatten
assert equal $processes [$process]

tg --url $local.url --token $alice.token sandbox destroy $other
