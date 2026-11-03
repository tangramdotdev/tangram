use ../../util/test.nu *

# A start that is queued for a runner fails when the runner's heartbeat expires, and the sandbox remains stopped.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
}

# Stop one sandbox, then fill the runner with another, so that a start of the first waits in the queue.
let stopped = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
tg --url $remote.url --token $root_token sandbox stop $stopped
let running = tg --url $remote.url --token $root_token sandbox create --no-tokens --cpu 1 | referent node
let enqueued = checkpoint watch scheduler.sandbox.enqueued --params { sandbox: $stopped } --token $root_token --url $remote.url
let start = job spawn {
	let job_id = job id
	tg --url $remote.url --token $root_token sandbox start $stopped | complete | job send --tag $job_id 0
}
checkpoint pass $enqueued
checkpoint unwatch $enqueued

# Kill the runner and advance the remote's clock past the runner ttl so that the scheduler expires it.
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
advance_time $remote 4sec

let output = job recv --tag $start --timeout 15sec
failure $output "the start should fail once the runner expires"
assert ($output.stderr | str contains "the sandbox's runner is unavailable")
assert equal (tg --url $remote.url --token $root_token sandbox get $stopped | from json | get data.status) stopped
