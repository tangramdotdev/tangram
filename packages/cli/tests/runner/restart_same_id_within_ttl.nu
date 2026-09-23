use ../lib/test.nu *

# A runner that dies and reconnects with the same id before its TTL elapses must have its old processes finished.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
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

let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
^tail --pid $pid -f /dev/null
let runner = server start $runner
let trivial = artifact { tangram.ts: 'export default () => 42' }
success (timeout 60s tg --url $local.url build --remote $trivial | complete) "the replacement runner must be registered"

let output = timeout 30s tg --url $local.url process wait $id | complete
assert equal $output.exit_code 0 "the old process must finish after the runner restarts"
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "finished"
