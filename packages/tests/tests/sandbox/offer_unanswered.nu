use ../../util/test.nu *

# A scheduler whose offer of a sandbox gets no answer from the runner sends the offer again until the runner answers, so the sandbox starts, a stop sent while the offer is unanswered completes, and the sandbox starts again.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { create_sandbox_timeout: 1 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Hold the runner before it answers the offer, so that the scheduler's wait for the answer times out.
let create = checkpoint watch runner.control.create_sandbox --url $runner.url
let unanswered = checkpoint watch scheduler.sandbox.offer.unanswered --token $root_token --url $remote.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
checkpoint wait $create | ignore

# Stop the sandbox while the offer is unanswered.
let url = $remote.url
let stop = job spawn {
	let id = job id
	timeout 30s tg --url $url --token $root_token sandbox stop --wait $sandbox | complete | job send --tag $id 0
}
checkpoint pass $unanswered | ignore
checkpoint unwatch $unanswered

# Let the runner answer. The stop completes once the runner holds the sandbox.
checkpoint unwatch $create
let output = job recv --tag $stop --timeout 30sec
success $output "the stop should succeed after the runner answers the offer"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped "the sandbox should be stopped"

# The sandbox starts again, because the scheduler no longer holds its first boot.
let output = timeout 30s tg --url $remote.url --token $root_token sandbox start --wait $sandbox | complete
success $output "the start should succeed"
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status started "the sandbox should be started"
assert equal $data.boot 1 "the sandbox should be at its second boot"
tg --url $remote.url --token $root_token sandbox destroy $sandbox
