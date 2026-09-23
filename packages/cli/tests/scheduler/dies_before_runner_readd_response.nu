use ../lib/test.nu *

# A scheduler that dies while re-adding a restarted runner, before the runner receives its new attempt, must expire the runner's old processes once it restarts.
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
	runner: { cpus: 1, id: $created.data.id, remote: "default", token: $created.token.token },
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
tg --url $remote.url --token $root_token checkpoint abort scheduler.runner.add --params $params
let remote_pid = open ($remote.directory | path join 'lock') | into int
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
^tail --pid $runner_pid -f /dev/null
let runner = server start $runner
wait_until --timeout 30sec { ps | where pid == $remote_pid | is-empty } "the scheduler must abort at the checkpoint"
let remote = server start $remote

success (timeout 60s tg --url $local.url process wait $id | complete) "the old process must finish after the scheduler restarts"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
assert equal $data.error.code "heartbeat_expiration"
