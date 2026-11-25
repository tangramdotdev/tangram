use ../../util/test.nu *

# A runner that restarts while its first connection ends the boots that it no longer holds has them ended by its next connection.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(600);
		}
	',
}
let id = tg --url $local.url build --wait --no-tokens --remote --detach $path | referent node
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"

# Kill the runner, start it again, and hold its connection once it has read the sandboxes that it no longer holds.
let restarted = checkpoint watch scheduler.runner.restarted --params { runner: $created.data.id } --token $root_token --url $remote.url
let pid = server_pid $runner.directory
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"
let runner = server start $runner
checkpoint wait $restarted 0 | ignore
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"

# Kill the runner again before its connection ends the boot, start it again, and let both connections continue, so that the newer connection ends the boot.
let pid = server_pid $runner.directory
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"
let runner = server start $runner
checkpoint wait $restarted 1 | ignore
checkpoint unwatch $restarted
let trivial = artifact { tangram.ts: 'export default () => 42' }
success (timeout 30s tg --url $local.url build --remote $trivial | complete) "new work must succeed after cleanup"
success (timeout 10s tg --url $local.url process wait $id | complete) "the previous process must finish"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
assert equal $data.exit 1
assert equal $data.error.node.code "internal"
assert equal $data.error.node.message "runner restarted"

assert equal (tg --url $remote.url --token $root_token sandbox get $data.sandbox | from json | get data.status) destroyed
