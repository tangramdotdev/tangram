use ../../util/test.nu *

# A process that a runner accepts into a running boot and that the runner never starts, because the runner dies, ends failed once the sandbox's scheduler declares the runner dead, and the sandbox is stopped.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let url = $remote.url

let path = artifact {
	tangram.ts: '
		export function hello() {
			return tg.command({
				args: ["-ec", "echo hello"],
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	',
}
let id = checkin push $remote $path --token $root_token
let command = tg --url $url --token $root_token build $'($id)#hello' | str trim
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node

# Spawn into the sandbox, and hold the runner once it has accepted the process.
let accepted = checkpoint watch runner.process.accepted --params { sandbox: $sandbox } --url $runner.url
let process = tg --url $url --token $root_token process spawn --no-tokens --verbose $'--sandbox=($sandbox)' $command | from json | get process
checkpoint wait $accepted | ignore

# Kill the runner, and advance the remote's clock past the runner ttl, so that the scheduler declares the runner dead.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid "the runner must exit"
advance_time $remote 4sec

# The process ends failed, and the sandbox is stopped.
let outcome = timeout 60s tg --url $url --token $root_token wait $process | from json
assert equal $outcome.exit 1 "the process should fail"
success (timeout 60s tg --url $url --token $root_token sandbox status --until stopped $sandbox | complete) "the sandbox should stop"
