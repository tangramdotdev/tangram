use ../lib/test.nu *

# A restarted runner destroys its previous sandboxes and finishes their processes before the scheduler TTL expires.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
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
			await tg.sleep(600);
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
success (timeout 60s tg --url $local.url build --remote $trivial | complete) "the restarted runner must accept work after cleanup"

let output = timeout 10s tg --url $local.url process wait $id | complete
assert equal $output.exit_code 0 "the old process must finish after the runner restarts"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
assert equal $data.exit 1
assert equal $data.error.code "internal"
assert equal $data.error.message "runner restarted"

assert equal (tg --url $remote.url --token $root_token sandbox get $data.sandbox | from json | get data.status) destroyed
