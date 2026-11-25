use ../../util/test.nu *

# A sandbox whose offer a runner did not answer before it restarted, and for which it wrote nothing, is placed again when the runner reconnects, without waiting for the runner's heartbeat to expire.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824, runner_ttl: 300 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: {
		cpus: 1,
		id: $created.data.id,
		overcommit: { cpu: 1 },
		remote: "default",
		token: $created.token.token,
	},
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the runner once it receives the offer, and crash it before it answers.
let offer = checkpoint watch runner.control.offer_boot --url $runner.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
checkpoint wait $offer | ignore
let pid = server_pid $runner.directory
kill --signal 9 $pid
wait_for_exit $pid

# The runner restarts within its heartbeat ttl, and the sandbox starts on it.
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token sandbox start --wait $sandbox | complete) "the sandbox should start after its runner restarts"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started
