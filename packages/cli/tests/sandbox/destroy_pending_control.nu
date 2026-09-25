use ../lib/test.nu *

# Destruction observes indexed completion while its control request remains pending.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	roles: [api indexer scheduler],
	sandbox: { status_wakeup_interval: 3600 },
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $owner.url sandbox create | str trim
let index = tg --url $owner.url checkpoint watch sandbox.destroy.index | from json | get watch
let control = tg --url $owner.url checkpoint watch sandbox.destroy.control | from json | get watch
let socket = $owner.url | str replace 'http+unix://' '' | url decode
let destroyer = job spawn {
	let job_id = job id
	let output = http post --allow-errors --full --max-time 20sec --unix-socket $socket --content-type application/json $'http://localhost/sandboxes/($sandbox)/destroy' '{}'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $owner.url checkpoint wait sandbox.destroy.index $index 0 | ignore
timeout 10s tg --url $owner.url checkpoint wait sandbox.destroy.control $control 0 | ignore
tg --url $runner.url sandbox destroy $sandbox
timeout 10s tg --url $owner.url sandbox wait --source=index $sandbox | ignore
tg --url $owner.url checkpoint unwatch sandbox.destroy.index $index
let output = job recv --tag $destroyer --timeout 5sec
assert equal $output.status 409
tg --url $owner.url checkpoint unwatch sandbox.destroy.control $control
