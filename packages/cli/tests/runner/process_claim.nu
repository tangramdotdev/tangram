use ../lib/test.nu *

# Only the runner that started a process may claim to act for it with the process header.

let root_token = random chars

# Spawn the remote and create two runners.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let other = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let header_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.header | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file);
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Release the scheduled parent's start and control header.
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete) "the parent should start"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.header $header_watch 0 | complete) "the parent should receive its control header"
tg --url $remote.url --token $root_token checkpoint continue process.control.header $header_watch 0

# Hold the child's control header, so that the child's control service answers while the runner has no token for the child.
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | from json
let child = $started.params.process
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
let header = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.header $header_watch 1 | from json
assert equal $header.params.process $child "the remote should hold the child's control header"

# Send a request with a token and a process header, and return the response status.
let socket = $remote.url | str replace 'http+unix://' '' | url decode
let script = "
import http.client, socket, sys
path, token, process = sys.argv[1:]
connection = http.client.HTTPConnection('localhost')
connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.sock.settimeout(30)
connection.sock.connect(path)
connection.request('GET', '/processes/' + process + '/status', headers={'Authorization': 'Bearer ' + token, 'X-Tg-Process': process})
print(connection.getresponse().status)
"
def claim [socket: string, script: string, token: string, process: string] {
	python3 -c $script $socket $token $process | str trim | into int
}

# The runner that started the child may claim it.
let status = claim $socket $script $created.token.token $child
assert ($status not-in [400 401 403]) $"the runner that started the child should be able to claim it, but got ($status)"

# Another runner, a user, and a malformed header are rejected.
assert equal (claim $socket $script $other.token.token $child) 403 "another runner should not be able to claim the child"
assert equal (claim $socket $script $alice.token $child) 403 "a user should not be able to claim the child"
assert equal (claim $socket $script $created.token.token 'invalid') 400 "a malformed process header should be rejected"

# Release the child's control header and let the build complete.
tg --url $remote.url --token $root_token checkpoint unwatch process.control.header $header_watch
let output = job recv --tag $build --timeout 30sec
success $output "the build should succeed"
