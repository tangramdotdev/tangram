use ../../util/test.nu *

# A sandbox that is booting when its scheduler dies is reported by its runner, so that the restarted scheduler does not place it twice.

let root_token = random chars
let config = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824, runner_ttl: 2 },
}
let remote = server spawn --name remote --config $config
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: {
		cpus: 1,
		id: $created.data.id,
		overcommit: { cpu: 1 },
		remote: "default",
		scheduler_ttl: 1,
		token: $created.token.token,
	},
}
wait_for_log $runner 'the runner is ready to accept work'

# Hold the sandbox's connection, and kill the scheduler while the sandbox is booting.
let connect = checkpoint watch sandbox.control.connect --token $root_token --url $remote.url
let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
checkpoint wait $connect | ignore
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let remote = server start $remote

# The sandbox connects to the restarted server, and is the only sandbox on the runner, so another sandbox waits.
loop {
	let status = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status
	if $status == 'started' { break }
	assert equal $status 'starting'
	sleep 100ms
}
let queued = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
sleep 500ms
assert equal (tg --url $remote.url --token $root_token sandbox get $queued | from json | get data.status) starting
tg --url $remote.url --token $root_token sandbox destroy --wait $sandbox
tg --url $remote.url --token $root_token sandbox destroy --wait $queued
