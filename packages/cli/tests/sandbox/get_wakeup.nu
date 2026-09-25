use ../lib/test.nu *

# A status notification restarts a read whose control result is stalled.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
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
tg --url $owner.url index
let watch = tg --url $owner.url checkpoint watch sandbox.get.control | from json | get watch
let reader = job spawn {
	let job_id = job id
	let output = timeout 10s tg --url $owner.url sandbox get $sandbox | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $owner.url checkpoint wait sandbox.get.control $watch 0 | ignore
tg --url $runner.url sandbox destroy $sandbox
let output = job recv --tag $reader --timeout 15sec
success $output
assert equal ($output.stdout | from json | get data.status) destroyed
tg --url $owner.url checkpoint unwatch sandbox.get.control $watch
