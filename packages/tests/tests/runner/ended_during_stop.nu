use ../../util/test.nu *

# A runner that learns that its scheduler ended a boot while the runner stops the boot gives the boot up and writes nothing more for it, so that the sandbox keeps the end that the scheduler recorded.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires the sandbox pool'
}
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
	runner: { id: $created.data.id, remote: "default", sandbox_pool_size: 1, scheduler_ttl: 1, token: $created.token.token },
}
wait_for_log $runner 'the runner is ready to accept work'
let url = $remote.url
let sandbox = tg --url $url --token $root_token sandbox create --wait --ttl 1h --no-tokens | referent node

# Stop the sandbox, and hold the runner's teardown of the boot once the sandbox has shut down.
let shutdown = checkpoint watch runner.sandbox.shutdown --params { sandbox: $sandbox } --url $runner.url
let stop = job spawn {
	timeout 60s tg --url $url --token $root_token sandbox stop $sandbox | complete | job send 0
}
checkpoint wait $shutdown | ignore
success (job recv --timeout 60sec) "the stop should be accepted"

# Hold the runner's heartbeats, and advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead and ends the boot while the runner stops it.
let heartbeat = checkpoint watch runner.control.heartbeat.received --params { runner: $created.data.id } --token $root_token --url $url
checkpoint wait $heartbeat 0 | ignore
let expired = checkpoint watch scheduler.runner.expired --params { runner: $created.data.id } --token $root_token --url $url
advance_time $remote 4sec
checkpoint pass $expired | ignore
checkpoint unwatch $expired
tg --url $url --token $root_token index
let ended = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $ended.status stopped "the scheduler should stop the sandbox when it declares the runner dead"

# Let the runner's teardown continue, and wait for the runner to discard the boot's state.
let removed = checkpoint watch runner.sandbox.state.removed --params { sandbox: $sandbox } --url $runner.url
checkpoint unwatch $heartbeat
checkpoint unwatch $shutdown
checkpoint pass $removed | ignore
checkpoint unwatch $removed

# The sandbox keeps the end that the scheduler recorded.
tg --url $url --token $root_token index
let data = tg --url $url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status stopped "the sandbox should stay stopped"
assert equal $data.error $ended.error "the sandbox should keep the scheduler's error"
assert equal $data.stopped_at $ended.stopped_at "the sandbox should keep the scheduler's stop"
