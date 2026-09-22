use ../lib/test.nu *

# Pooled connections replay initialization after a remote restart.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		process_control_connection_pool_size: 1,
		remote: 'default',
		sandbox_control_connection_pool_size: 1,
		token: $created.token.token,
	},
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold sandbox creation and process start on the remote.
let sandbox_start_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.create.received | from json | get watch
let process_start_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.start.received | from json | get watch

let path = artifact {
	"example.tg.ts": '
		export default async () => tg.run(await tg.file({ contents: "#!/bin/sh\nprintf 42", executable: true })).sandbox(true);
	'
}
let spawned = tg --url $local.url build --remote --detach --verbose --user $alice.user.id $"($path)/example.tg.ts" | from json
let process = $spawned.process | split row '?' | first

# The remote receives both requests and is held before indexing either.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.create.received $sandbox_start_watch 0 | complete
success $output "the remote should receive the sandbox create request"
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.start.received $process_start_watch 0 | complete
success $output "the remote should receive the process start"

# Crash the remote without gracefully removing its runners, then replay the unacknowledged initialization.
let pid = open --raw ($remote.directory | path join lock) | str trim | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}
let remote = server start $remote

let output = timeout 60s tg --url $local.url wait --remote $process | complete
success $output "the run should succeed after the starts replay"
let output = $output.stdout | from json
assert equal $output.exit 0 ($output | to json)
