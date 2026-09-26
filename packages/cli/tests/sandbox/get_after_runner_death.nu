use ../lib/test.nu *

# A sandbox get issued after its runner dies must observe destruction without waiting for the control read timeout.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	control: { read_timeout: 60 },
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
if $nu.os-info.name == "linux" {
	^tail --pid $runner_pid -f /dev/null
} else {
	while (ps | where pid == $runner_pid | is-not-empty) { sleep 10ms }
}

# Allow the runner TTL and the next scheduler cleanup tick, but not the control read timeout.
let output = timeout 15s tg --url $remote.url --token $root_token sandbox get $sandbox | complete
success $output "the sandbox get must complete before the control read timeout"
assert equal ($output.stdout | from json | get data.status) "destroyed" "the pending get must observe destruction once the runner expires"
