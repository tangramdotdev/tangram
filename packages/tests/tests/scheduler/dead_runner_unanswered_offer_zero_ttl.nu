use ../../util/test.nu *

# A sandbox with a zero ttl whose offer to a runner is unanswered when the scheduler declares the runner dead is destroyed, because it cannot stay stopped.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the runner once it receives the offer of a sandbox with a zero ttl, and kill it before it answers.
let offer = checkpoint watch runner.control.offer_boot --url $runner.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens --ttl 0s | referent node
checkpoint wait $offer | ignore
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"

# Advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead with the offer unanswered.
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $remote.url
advance_time $remote 4sec
checkpoint pass $expired | ignore
checkpoint unwatch $expired

# The sandbox is destroyed.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $sandbox | complete) "the sandbox should be destroyed"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
