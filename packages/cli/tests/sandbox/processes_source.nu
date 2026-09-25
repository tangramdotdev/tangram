use ../lib/test.nu *

# Listings preserve their source, pagination, and IDs after individual runner processes retire.
let owner = server spawn --name owner --config {
	control: { read_timeout: 0.25 },
	roles: [api indexer scheduler],
	sandbox: { processes_wakeup_interval: 3600 },
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	control: { read_timeout: 0.25 },
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, process_state_ttl: 0, remote: default, sandbox_state_ttl: 60, token: $created.token.token },
}
let slow = server spawn --name slow --config { control: { read_timeout: 60 } }
let client = server spawn --name client --config {
	remotes: { alpha: { url: $slow.url }, zeta: { url: $owner.url } },
}
let sandbox = tg --url $owner.url sandbox create | str trim
let runner_socket = $runner.url | str replace 'http+unix://' '' | url decode
let owner_socket = $owner.url | str replace 'http+unix://' '' | url decode
let client_socket = $client.url | str replace 'http+unix://' '' | url decode
let path = artifact { tangram.ts: 'export default () => "done";' }
let reader = job spawn {
	let job_id = job id
	http get --raw --max-time 30sec --unix-socket $runner_socket $'http://localhost/sandboxes/($sandbox)/processes?source=runner&size=1&length=2'
	| lines
	| where { ($in starts-with 'data: ') or $in == 'event: end' }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
let first = tg --url $owner.url spawn $'--sandbox=($sandbox)' $path | str trim
let first_chunk = job recv --tag $reader --timeout 10sec | str substring 6.. | from json
assert equal $first_chunk { data: [$first], position: 0 }
timeout 10s tg --url $owner.url wait --source=index $first | ignore
let second = tg --url $owner.url spawn $'--sandbox=($sandbox)' $path | str trim
let second_chunk = job recv --tag $reader --timeout 10sec | str substring 6.. | from json
assert equal $second_chunk { data: [$second], position: 1 }
assert equal (job recv --tag $reader --timeout 10sec) 'event: end'
timeout 10s tg --url $owner.url wait --source=index $second | ignore
wait_until {
	(tg --url $runner.url process get --source=runner $first | complete | get exit_code) != 0
} --timeout 10sec 'the individual runner process state should retire'
tg --url $owner.url index
for source in [auto runner] {
	for socket in [$runner_socket $owner_socket $client_socket] {
		let listing = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?source=($source)&timeout=0&size=1'
		let chunks = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
		assert equal ($chunks | get data | flatten) [$first $second]
		assert equal ($chunks | get position) [0 1]
		let tail = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?source=($source)&timeout=0&position=end.-1&length=1'
		let chunk = $tail | lines | where { $in starts-with 'data: ' } | first | str substring 6.. | from json
		assert equal $chunk { data: [$second], position: 1 }
	}
}
tg --url $owner.url sandbox destroy $sandbox
timeout 10s tg --url $owner.url sandbox wait --source=index $sandbox | ignore
for socket in [$runner_socket $owner_socket $client_socket] {
	let listing = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/processes?source=index&size=1'
	let chunks = $listing | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
	assert equal ($chunks | get data | flatten) [$first $second]
	assert equal ($chunks | get position) [0 1]
}
let runner = server restart $runner
let output = http get --allow-errors --full --max-time 10sec --unix-socket $owner_socket $'http://localhost/sandboxes/($sandbox)/processes?source=runner&timeout=0'
assert equal $output.status 404
let listing = http get --raw --max-time 10sec --unix-socket $owner_socket $'http://localhost/sandboxes/($sandbox)/processes?source=index'
assert ($listing | str contains $first)
assert ($listing | str contains $second)
assert ($listing | str contains 'event: end')
