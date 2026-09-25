use ../lib/test.nu *

# A scheduler that dies before expiring a restarted runner's old processes must expire them once it restarts.
let root_token = random chars
let remote = server spawn --name remote --preserve-keys --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 10 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --preserve-keys --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 2, id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(120);
		}
	',
}
let id = tg --url $local.url build --remote --detach $path | str trim
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"

let params = { runner: $created.data.id } | to json --raw
let reconcile_watch = tg --url $remote.url --token $root_token checkpoint watch scheduler.runner.reconcile --params $params | from json | get watch
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
^tail --pid $runner_pid -f /dev/null
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait scheduler.runner.reconcile $reconcile_watch 0 | complete) "the scheduler must reconcile the restarted runner"
let trivial = artifact { tangram.ts: 'export default () => 42' }
success (timeout 60s tg --url $local.url build --remote $trivial | complete) "the restarted runner must be added before the scheduler dies"

let remote_pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $remote_pid
^tail --pid $remote_pid -f /dev/null
let remote = server start $remote

success (timeout 60s tg --url $local.url process wait $id | complete) "the old process must finish after the scheduler restarts"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
assert equal $data.error.code "heartbeat_expiration"
