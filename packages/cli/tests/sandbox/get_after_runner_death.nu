use ../lib/test.nu *

# A sandbox get issued after its runner dies must return promptly instead of waiting on the runner, and must report the sandbox as destroyed once the runner expires.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
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
let sandbox = tg --url $remote.url --token $root_token get $id | from json | get sandbox
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) "started"

let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
^tail --pid $runner_pid -f /dev/null

let output = timeout 5s tg --url $remote.url --token $root_token sandbox get $sandbox | complete
success $output "the sandbox get must return promptly after the runner dies"
wait_until --timeout 30sec {
	(tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) == "destroyed"
} "the sandbox must be destroyed once the runner expires"
