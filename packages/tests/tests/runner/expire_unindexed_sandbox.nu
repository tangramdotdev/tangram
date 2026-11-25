use ../../util/test.nu *

# A sandbox whose runner records it and is lost before the record is indexed is destroyed when the scheduler removes the runner, and its process finishes with the expiration's error.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, id: $created.data.id, overcommit: { cpu: 1 }, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(1000);
		}
	'
}

# Spawn a process with a new sandbox, and hold the sandbox's control connection before the runner's record of the sandbox is submitted.
let connect = checkpoint watch sandbox.control.connect --token $root_token --url $remote.url
let submitted = checkpoint watch sandbox.control.index.submitted --token $root_token --url $remote.url
let process = tg --url $local.url build --no-tokens --remote --detach $path | referent node
let sandbox = checkpoint wait $connect | get params.sandbox
tg --url $remote.url --token $root_token index

# Hold the index, and let the connection submit the runner's record of the sandbox, which is then not indexed.
let write = checkpoint watch index.write --token $root_token --url $remote.url
checkpoint unwatch $connect
checkpoint wait $submitted | ignore
let index = checkpoint watch scheduler.runner.index --params { runner: $created.data.id } --token $root_token --url $remote.url

# Kill the runner and advance the remote's clock past the runner ttl, so that the scheduler removes the runner.
let runner_pid = server_pid $runner.directory
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
advance_time $remote 4sec

# Release the index once the scheduler is about to await indexing after it removes the runner.
checkpoint pass $index | ignore
checkpoint unwatch $index
checkpoint unwatch $write
checkpoint unwatch $submitted

# The sandbox is destroyed, and its process finishes with the expiration's error.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
tg --url $remote.url --token $root_token index
let data = tg --url $remote.url --token $root_token process get $process | from json
assert equal $data.status finished
assert equal $data.exit 1
assert equal $data.error.node.message 'heartbeat expired'
