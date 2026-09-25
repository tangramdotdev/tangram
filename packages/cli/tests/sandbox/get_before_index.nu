use ../lib/test.nu *

# A missing index result must not end a read while control is becoming available.
let owner = server spawn --name owner --config {
	advanced: { checkpoints: true },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
}
let created = tg --url $owner.url runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { url: $owner.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let connect = tg --url $owner.url checkpoint watch sandbox.control.connect | from json | get watch
let creator = job spawn {
	let job_id = job id
	let output = timeout 20s tg --url $owner.url sandbox create | complete
	$output | job send --tag $job_id 0
}
let sandbox = timeout 10s tg --url $owner.url checkpoint wait sandbox.control.connect $connect 0 | from json | get params.sandbox
let index = tg --url $owner.url checkpoint watch sandbox.get.index | from json | get watch
let reader = job spawn {
	let job_id = job id
	let output = timeout 20s tg --url $owner.url sandbox get $sandbox | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $owner.url checkpoint wait sandbox.get.index $index 0 | ignore
tg --url $owner.url checkpoint unwatch sandbox.get.index $index
assert equal (try { job recv --tag $reader --timeout 300ms } catch { null }) null
tg --url $owner.url checkpoint unwatch sandbox.control.connect $connect
success (job recv --tag $creator --timeout 20sec)
let output = job recv --tag $reader --timeout 20sec
success $output
assert equal ($output.stdout | from json | get data.status) started
tg --url $owner.url sandbox destroy $sandbox
