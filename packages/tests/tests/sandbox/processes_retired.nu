use ../../util/test.nu *

# Listings preserve their pagination and IDs after individual runner processes retire.
let local_owner = server spawn --name local-owner --config {
	control: { read_timeout: 0.25 },
	roles: [api indexer scheduler],
	sandbox: { processes_wakeup_interval: 3600 },
}
let created = tg --url $local_owner.url runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 0.25 },
	remotes: { default: { url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, process_state_time_to_live: 0, remote: default, sandbox_state_time_to_live: 60, token: $created.token.token },
}
let remote_slow = server spawn --name remote-slow --config { control: { read_timeout: 60 } }
let local_client = server spawn --name local-client --config {
	remotes: { alpha: { url: $remote_slow.url }, zeta: { url: $local_owner.url } },
}
let sandbox = tg --url $local_owner.url sandbox create --no-tokens | referent node
let runner_socket = $runner.url | str replace 'http+unix://' '' | url decode
let local_owner_socket = $local_owner.url | str replace 'http+unix://' '' | url decode
let local_client_socket = $local_client.url | str replace 'http+unix://' '' | url decode
let path = artifact { tangram.ts: 'export default () => "done";' }
let removed = checkpoint watch runner.process.state.removed --url $runner.url
let reader = job spawn {
	let job_id = job id
	http get --raw --max-time 30sec --unix-socket $runner_socket $'http://localhost/sandboxes/($sandbox)/processes?size=1&length=2'
	| lines
	| where { ($in starts-with 'data: ') or $in == 'event: end' }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
let first = tg --url $local_owner.url spawn --no-tokens $'--sandbox=($sandbox)' $path | referent node
let first_chunk = job recv --tag $reader --timeout 10sec | str substring 6.. | from json
assert equal $first_chunk { data: [$first], position: 0 }
timeout 10s tg --url $local_owner.url wait $first | ignore
tg --url $local_owner.url index
let second = tg --url $local_owner.url spawn --no-tokens $'--sandbox=($sandbox)' $path | referent node
let second_chunk = job recv --tag $reader --timeout 10sec | str substring 6.. | from json
assert equal $second_chunk { data: [$second], position: 1 }
assert equal (job recv --tag $reader --timeout 10sec) 'event: end'
timeout 10s tg --url $local_owner.url wait $second | ignore
tg --url $local_owner.url index

# Wait for the runner to discard the state of both processes.
let processes = [0 1] | each { |hit| checkpoint wait $removed $hit | get params.process }
checkpoint unwatch $removed
assert equal ($processes | sort) ([$first $second] | sort) 'the individual runner process state should retire'
tg --url $local_owner.url index
for socket in [$runner_socket $local_owner_socket $local_client_socket] {
	let listing = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?timeout=0&size=1'
	let chunks = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
	assert equal ($chunks | get data | flatten) [$first $second]
	assert equal ($chunks | get position) [0 1]
	let tail = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?timeout=0&position=end.-1&length=1'
	let chunk = $tail | lines | where { $in starts-with 'data: ' } | first | str substring 6.. | from json
	assert equal $chunk { data: [$second], position: 1 }
}
tg --url $local_owner.url sandbox destroy $sandbox
timeout 10s tg --url $local_owner.url sandbox wait $sandbox | ignore
tg --url $local_owner.url index
for socket in [$runner_socket $local_owner_socket $local_client_socket] {
	let listing = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?size=1'
	let chunks = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
	assert equal ($chunks | get data | flatten) [$first $second]
	assert equal ($chunks | get position) [0 1]
}
let runner = server restart $runner
let listing = http get --raw --max-time 10sec --unix-socket $local_owner_socket $'http://localhost/sandboxes/($sandbox)/processes'
assert ($listing | str contains $first)
assert ($listing | str contains $second)
assert ($listing | str contains 'event: end')
