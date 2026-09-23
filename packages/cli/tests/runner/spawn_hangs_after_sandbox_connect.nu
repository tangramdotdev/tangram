use ../lib/test.nu *

# A spawn whose runner dies after the sandbox connects but before the process connects must return an error instead of hanging.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, id: $created.data.id, remote: "default", token: $created.token.token },
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

success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $connect_watch 0 | complete) "the runner must reach the process control connect checkpoint"
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.spawn.connection.wait $wait_watch 0 | complete) "the spawner must reach the connection wait checkpoint"

let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
^tail --pid $runner_pid -f /dev/null
tg --url $remote.url --token $root_token checkpoint continue process.spawn.connection.wait $wait_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.spawn.connection.wait $wait_watch

let output = try { job recv --tag $build --timeout 15sec } catch { null }
assert ($output != null) "the spawn must return after the runner dies before the process connects"
failure $output
