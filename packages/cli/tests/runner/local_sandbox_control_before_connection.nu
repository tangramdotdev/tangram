use ../lib/test.nu *

# A scheduled sandbox can be destroyed before its control connection returns.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let control_watch = tg --url $runner.url checkpoint watch runner.sandbox.control.connect | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.sandbox.state.inserted | from json | get watch
let create = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token sandbox create | complete
	$output | job send --tag $job_id 0
}
let sandbox = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 0 | from json | get params.sandbox
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch
timeout 10s tg --url $runner.url checkpoint wait runner.sandbox.control.connect $control_watch 0 | ignore
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let status = job spawn {
	let job_id = job id
	http get --max-time 30sec --raw --unix-socket $socket $'http://localhost/sandboxes/($sandbox)/status?location=remote'
	| lines
	| where { $in in ['data: "started"' 'data: "destroyed"' 'event: end'] }
	| each { $in | job send --tag $job_id 0 }
	| ignore
}
assert equal (job recv --tag $status --timeout 10sec) 'data: "started"'
let headers = { 'Content-Type': 'application/json' }
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"remote"}'
assert equal $output.status 200 "local destruction must not wait for the sandbox connection"
assert equal (job recv --tag $status --timeout 10sec) 'data: "destroyed"'
assert equal (job recv --tag $status --timeout 10sec) 'event: end'
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket --headers $headers $'http://localhost/sandboxes/($sandbox)/destroy' '{"location":"remote"}'
assert equal $output.status 409

tg --url $runner.url checkpoint unwatch runner.sandbox.control.connect $control_watch
success (job recv --tag $create --timeout 30sec)
assert equal (timeout 30s tg --url $remote.url --token $root_token sandbox wait $sandbox | from json) destroyed
