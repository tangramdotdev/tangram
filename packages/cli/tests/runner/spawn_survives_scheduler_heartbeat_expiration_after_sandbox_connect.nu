use ../lib/test.nu *

# Once the sandbox connects, scheduler heartbeat expiration must not cancel a spawn that is still waiting for its process connection.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { heartbeat_interval: 60, heartbeat_ttl: 3 },
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
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.spawn.connection.wait $wait_watch 0 | complete) "the sandbox must connect before the scheduler heartbeat expires"
tg --url $remote.url --token $root_token checkpoint unwatch process.spawn.connection.wait $wait_watch

let output = try { job recv --tag $build --timeout 5sec } catch { null }
assert ($output == null) "scheduler heartbeat expiration must not fail a spawn after the sandbox connects"

tg --url $runner.url checkpoint unwatch runner.process.control.connect $connect_watch
let output = job recv --tag $build --timeout 30sec
success $output "the build must complete after process control connects"
assert equal ($output.stdout | str trim) "42"
