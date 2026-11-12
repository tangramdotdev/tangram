use ../../util/test.nu *

# A sandbox that a runner lost when it restarted is placed again when the runner reconnects, without waiting for the runner's heartbeat to expire.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824, runner_ttl: 300 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
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

# Hold the sandbox's connection, and crash the runner while the sandbox is booting.
let connect = checkpoint watch sandbox.control.connect --token $root_token --url $remote.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
checkpoint wait $connect | ignore
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
checkpoint unwatch $connect

# The runner restarts within its heartbeat time_to_live, and the sandbox starts on it.
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token sandbox start --wait $sandbox | complete) "the sandbox should start after its runner restarts"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started
