use ../lib/test.nu *

# A spawn whose runner dies after the sandbox connects but before the process connects must observe destruction without waiting for the control read timeout.
let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	control: { read_timeout: 60 },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let connect_watch = tg --url $runner.url checkpoint watch runner.process.control.connect | from json | get watch
let wait_watch = tg --url $remote.url --token $root_token checkpoint watch process.spawn.connection.wait | from json | get watch

let path = artifact { tangram.ts: 'export default () => 42' }
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.control.connect $connect_watch 0 | complete) "the runner must reach the process control connect checkpoint"
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.spawn.connection.wait $wait_watch 0 | complete) "the spawner must reach the connection wait checkpoint"

let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
tg --url $remote.url --token $root_token checkpoint continue process.spawn.connection.wait $wait_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.spawn.connection.wait $wait_watch

# Advance the remote's clock past the runner ttl so that the scheduler expires the dead runner.
advance_time $remote 4sec

let output = try { job recv --tag $build --timeout 15sec } catch { null }
assert ($output != null) "the spawn must return after the runner dies before the process connects"
failure $output
assert ($output.stderr | ansi strip | str contains "the sandbox was destroyed") "the spawn must report sandbox destruction"
