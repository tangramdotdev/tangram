use ../../util/test.nu *

# A process from a sandbox's previous boot remains readable after the sandbox stops and starts again, on a local runner and on a remote runner.

let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log("before stop");
			await tg.sleep(120);
		}
	',
}
let root_token = random chars
let local = server spawn --name local
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

for server in [{ url: $local.url, token: null, id: null }, { url: $remote.url, token: $root_token, id: (checkin push $remote $path --token $root_token) }] {
	let connection = [--url $server.url ...(if $server.token == null { [] } else { [--token $server.token] })]
	let command = if $server.id == null { $path } else { $server.id }
	let sandbox = tg ...$connection sandbox create --no-tokens | referent node
	let process = tg ...$connection run --no-tokens --detach $"--sandbox=($sandbox)" $command | referent node

	# Wait for the process's log, then stop and start the sandbox.
	let expected = "before stop\n"
	let length = $expected | str length --utf-8-bytes
	let output = timeout 30s tg ...$connection log --no-timeout --stream stdout --length $length $process | complete
	success $output "the log should be readable before the stop"
	tg ...$connection sandbox stop $sandbox
	tg ...$connection sandbox start $sandbox

	# The previous boot's process finished with an error, and its log is still readable.
	let output = timeout 30s tg ...$connection wait $process | complete
	success $output "the previous boot's process should be waitable"
	assert (($output.stdout | from json).error? != null) "the previous boot's process should finish with an error"
	let output = timeout 30s tg ...$connection log $process | complete
	success $output "the previous boot's log should be readable"
	assert equal $output.stdout $expected
}
