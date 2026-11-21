use ../../util/test.nu *

# A process on its way into an existing sandbox runs after its scheduler restarts between recording the process and offering it to the sandbox's runner, because the process is in the scheduler's saved queue.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", scheduler_ttl: 1, token: $created.token.token },
}

let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({ args: ["-ec", "echo hello"], executable: "/bin/sh", host: tg.host.current });
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $remote.url --token $root_token build $'($id)#hello' | str trim
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node

# Spawn into the sandbox, and hold the scheduler after it records the process and before it offers the process to the sandbox's runner.
let request = checkpoint watch scheduler.process.spawn --params { sandbox: $sandbox } --token $root_token --url $remote.url
let url = $remote.url
job spawn {
	tg --url $url --token $root_token spawn --no-tokens $'--sandbox=($sandbox)' $command | complete | ignore
}
checkpoint wait $request 0 | ignore
tg --url $remote.url --token $root_token index
let queued = tg --url $remote.url --token $root_token ps --status queued | from json
assert equal ($queued | length) 1 'the process should be queued'
let process = $queued | first | get node

# Kill the scheduler and restart it with the same ID.
let pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let remote = server start $remote

# The restarted scheduler offers the process again, and the sandbox's runner runs it.
let outcome = tg --url $remote.url --token $root_token wait $process | from json
assert equal $outcome.exit 0 'the process should run in the sandbox'
let data = tg --url $remote.url --token $root_token process get $process | from json
assert equal $data.status finished
assert equal $data.sandbox $sandbox
