use ../../util/test.nu *

# A sandbox that a scheduler accepted survives the scheduler's restart, so that it starts once a runner has capacity.

let root_token = random chars
let config = {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824, runner_time_to_live: 2 },
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
		scheduler_time_to_live: 1,
		token: $created.token.token,
	},
}

# Fill the runner, so that the next sandbox waits in the queue.
let running = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
assert equal (tg --url $remote.url --token $root_token sandbox get $queued | from json | get data.status) starting

# Kill the scheduler and restart it with the same ID.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let remote = server start $remote

# The queued sandbox starts once the capacity is released.
tg --url $remote.url --token $root_token sandbox destroy --wait $running
loop {
	let status = tg --url $remote.url --token $root_token sandbox get $queued | from json | get data.status
	if $status == 'started' { break }
	assert equal $status 'starting'
	sleep 100ms
}
