use ../../util/test.nu *

# A sandbox that is queued survives a restart of its scheduler, and can then be destroyed, while the running sandbox is untouched.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824, runner_time_to_live: 2 },
}
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
let running = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node

# Kill the scheduler and restart it with the same ID.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let remote = server start $remote

# Destroying the queued sandbox waits for the recovery and removes it, and nothing is placed twice.
tg --url $remote.url --token $root_token sandbox destroy --wait $queued
assert equal (tg --url $remote.url --token $root_token sandbox get $queued | from json | get data.status) destroyed
assert equal (tg --url $remote.url --token $root_token sandbox get $running | from json | get data.status) started
tg --url $remote.url --token $root_token sandbox destroy --wait $running
