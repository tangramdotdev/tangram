use ../lib/test.nu *

# A runner that restarts during startup cleanup retries using the indexed sandboxes.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
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

let sandbox = tg --url $remote.url --token $root_token get $id | from json | get sandbox
let params = { sandbox: $sandbox } | to json --raw
let watch = tg --url $remote.url --token $root_token checkpoint watch runner.control.destroy_sandbox --params $params | from json | get watch
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
^tail --pid $pid -f /dev/null
let runner = server start $runner
success (timeout 10s tg --url $remote.url --token $root_token checkpoint wait runner.control.destroy_sandbox $watch 0 | complete) "startup must clean up the old sandbox"
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"

# Interrupt the runner before cleanup is acknowledged, so startup must retry using the index.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
^tail --pid $pid -f /dev/null
tg --url $remote.url --token $root_token checkpoint continue runner.control.destroy_sandbox $watch 0
tg --url $remote.url --token $root_token checkpoint unwatch runner.control.destroy_sandbox $watch
let runner = server start $runner
let trivial = artifact { tangram.ts: 'export default () => 42' }
success (timeout 30s tg --url $local.url build --remote $trivial | complete) "new work must succeed after cleanup"
success (timeout 10s tg --url $local.url process wait $id | complete) "the previous process must finish"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
assert equal $data.exit 1
assert equal $data.error.code "internal"
assert equal $data.error.message "runner restarted"

assert equal (tg --url $remote.url --token $root_token sandbox get $data.sandbox | from json | get data.status) destroyed
