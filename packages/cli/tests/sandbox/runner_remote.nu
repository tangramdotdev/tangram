use ../../test.nu *

# Runner-backed sandbox reads do not wait for owner-side registration or destruction.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	indexer: { cleaning: {} },
	roles: [api indexer scheduler],
	sandbox: { ttl: 0 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
	sandbox: { status_wakeup_interval: 3600.0 },
}
let connect_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.connect | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.sandbox.state.inserted | from json | get watch
let create_job = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token sandbox create | complete
	$output | job send --tag $job_id 0
}
timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 0 | ignore
tg --url $runner.url checkpoint continue runner.sandbox.state.inserted $state_watch 0
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch
let sandbox = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.connect $connect_watch 0 | from json | get params.sandbox
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let query = { location: 'remote(hint)' } | url build-query
let output = http get --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)?($query)'
assert equal $output.location remote
assert equal $output.data.status started
assert ($output.tokens.local? | is-not-empty)
assert ($output.tokens.remote? | is-empty) "the runner's own remote capability must not be returned"
let status = http get --max-time 10sec --raw --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?($query)&timeout=0'
assert ($status | str contains started)

tg --url $remote.url --token $root_token checkpoint continue sandbox.control.connect $connect_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $connect_watch
success (job recv --tag $create_job --timeout 10sec)

# Status completion uses runner notifications, not the delayed destroy commit.
let destroy_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.destroy | from json | get watch
let wait_job = job spawn {
	let job_id = job id
	http get --max-time 30sec --raw --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?($query)'
	| lines
	| where { $in in ['data: "started"' 'data: "destroyed"' 'event: end'] }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
assert equal (job recv --tag $wait_job --timeout 10sec) 'data: "started"'
tg --url $remote.url --token $root_token sandbox destroy $sandbox
timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.destroy $destroy_watch 0 | ignore
assert equal (job recv --tag $wait_job --timeout 10sec) 'data: "destroyed"'
assert equal (job recv --tag $wait_job --timeout 10sec) 'event: end'
tg --url $remote.url --token $root_token checkpoint continue sandbox.control.destroy $destroy_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.destroy $destroy_watch

# Retained remote runner state must not answer new requests after the sandbox expires.
wait_until { (tg --url $remote.url --token $root_token sandbox get $sandbox | complete | get exit_code) != 0 } --timeout 15sec "the sandbox should expire"
let status = http get --allow-errors --full --max-time 10sec --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?location=remote&timeout=0'
assert equal $status.status 404
let output = timeout 10s tg --url $runner.url sandbox wait --remote $sandbox | complete
failure $output
assert ($output.stderr | str contains "failed to find the sandbox")
