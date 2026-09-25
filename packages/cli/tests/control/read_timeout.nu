use ../lib/test.nu *

# A sandbox read falls back to the index when control cannot answer within the configured deadline.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 0.25 },
	roles: [api indexer scheduler],
	sandbox: { status_wakeup_interval: 0.05 },
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let sandbox = tg --url $owner.url sandbox create | str trim
tg --url $owner.url index
let watch = tg --url $owner.url checkpoint watch sandbox.control.request | from json | get watch
let reader = job spawn {
	let job_id = job id
	let output = timeout 5s tg --url $owner.url sandbox get $sandbox | complete
	$output | job send --tag $job_id 0
}
timeout 5s tg --url $owner.url checkpoint wait sandbox.control.request $watch 0 | ignore
let output = job recv --tag $reader --timeout 10sec
success $output
assert equal ($output.stdout | from json | get data.status) started
tg --url $owner.url checkpoint unwatch sandbox.control.request $watch
tg --url $owner.url sandbox destroy $sandbox
