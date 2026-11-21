use ../../util/test.nu *

# A spawn into a running sandbox returns once the runner accepts the process, without waiting for the scheduler's record of the process, and the process runs.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
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

# Hold the scheduler's record of the spawn, and spawn into the sandbox. The spawn returns, and the process runs.
let record = checkpoint watch scheduler.process.record --params { sandbox: $sandbox } --token $root_token --url $url
let process = timeout 60s tg --url $url --token $root_token process spawn --no-tokens --verbose $'--sandbox=($sandbox)' $command | from json | get process
checkpoint wait $record | ignore
let outcome = timeout 60s tg --url $url --token $root_token wait $process | from json
assert equal $outcome.exit 0 "the process should succeed"

# Let the record continue.
checkpoint unwatch $record
