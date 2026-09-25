use ../lib/test.nu *

# A sandbox dispatched to a restarted runner while its previous attempt is being expired must survive the expiry.
let root_token = random chars
let remote = server spawn --name remote --config {
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
let old = tg --url $local.url build --remote --detach $path | str trim
assert equal (tg --url $remote.url --token $root_token get $old | from json | get status) "started"

let params = { runner: $created.data.id } | to json --raw
let reconcile_watch = tg --url $remote.url --token $root_token checkpoint watch scheduler.runner.reconcile --params $params | from json | get watch
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
^tail --pid $pid -f /dev/null
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait scheduler.runner.reconcile $reconcile_watch 0 | complete) "the scheduler must reconcile the restarted runner"
let trivial = artifact { tangram.ts: 'export default () => 42' }
let new = tg --url $local.url build --remote --detach $trivial | str trim
success (timeout 60s tg --url $local.url process wait $new | complete) "the new build must run on the restarted runner"
tg --url $remote.url --token $root_token checkpoint continue scheduler.runner.reconcile $reconcile_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch scheduler.runner.reconcile $reconcile_watch

success (timeout 30s tg --url $local.url process wait $old | complete) "the old process must finish after the runner restarts"
let old = tg --url $remote.url --token $root_token get $old | from json
assert equal $old.status "finished"
assert equal $old.error.code "heartbeat_expiration"
let new = tg --url $remote.url --token $root_token get $new | from json
assert equal $new.status "finished"
assert equal $new.exit 0 "the new process must not be expired"
