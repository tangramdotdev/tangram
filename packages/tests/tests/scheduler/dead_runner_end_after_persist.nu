use ../../util/test.nu *

# A sandbox whose offer to a runner is unanswered when the scheduler declares the runner dead, before the scheduler's first write of the sandbox applies, is stopped once that write applies, because the end waits for the sandbox's first record.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the scheduler's first write of a new sandbox, and hold the runner once it receives the boot's offer.
let persist = checkpoint watch scheduler.sandbox.persist --token $root_token --url $remote.url
let offer = checkpoint watch runner.control.offer_boot --url $runner.url
let url = $remote.url
let create = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox create --no-tokens | complete | job send --tag $id 0
}
let sandbox = checkpoint wait $persist | get params.sandbox
checkpoint wait $offer | ignore

# Kill the runner before it answers, and delete it, so that the scheduler ends the boot of the unanswered offer.
let pid = server_pid $runner.directory
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"
success (timeout 60s tg --url $remote.url --token $root_token runner delete $created.data.id | complete) "the runner should be deleted"

# Release the first write. The sandbox is stopped rather than left starting.
checkpoint unwatch $persist
job recv --tag $create --timeout 60sec | ignore
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until stopped $sandbox | complete) "the sandbox should be stopped"
