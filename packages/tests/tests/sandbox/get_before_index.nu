use ../../util/test.nu *

# A read of a sandbox whose runner has not connected returns the record that the scheduler wrote.
let local_owner = server spawn --name local-owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
}
let created = tg --url $local_owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $local_owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let connect = tg --url $local_owner.url checkpoint watch sandbox.control.connect | from json | get watch
let creator = job spawn {
	let job_id = job id
	let output = timeout 20s tg --url $local_owner.url sandbox create --no-tokens | complete
	$output | job send --tag $job_id 0
}
let sandbox = tg --url $local_owner.url checkpoint wait --timeout 10s sandbox.control.connect $connect 0 | from json | get params.sandbox
let output = timeout 20s tg --url $local_owner.url sandbox get $sandbox | complete
success $output
assert equal ($output.stdout | from json | get data.status) starting
tg --url $local_owner.url checkpoint unwatch sandbox.control.connect $connect
success (job recv --tag $creator --timeout 20sec)
tg --url $local_owner.url sandbox destroy --wait $sandbox
