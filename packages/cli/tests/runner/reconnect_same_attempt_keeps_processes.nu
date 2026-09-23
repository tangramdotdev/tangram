use ../lib/test.nu *

# A runner that reconnects with its current attempt after the server restarts must keep its processes running.
let root_token = random chars
let remote = server spawn --name remote --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
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

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.sleep(10);
			return 42;
		}
	',
}
let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let id = tg --url $local.url build --remote --detach $path | str trim
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"

success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete) "the process must start on the runner"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

let remote_pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $remote_pid
^tail --pid $remote_pid -f /dev/null
let remote = server start $remote

let output = timeout 60s tg --url $local.url process wait $id | complete
success $output "the process must finish after the runner reconnects"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
let error = $data | get -o error
let error = if ($error | describe) == "string" { tg --url $remote.url --token $root_token get $error } else { $error | to json --raw }
assert equal $data.exit 0 $"the process must not be expired: ($error)"
