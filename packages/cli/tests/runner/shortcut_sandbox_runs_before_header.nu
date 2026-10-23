use ../lib/test.nu *

# A shortcut sandbox becomes active before its control connection receives its header, and its process completes once the connection is released.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold the shortcut sandbox's connection on the remote, and watch the sandboxes become active on the runner.
let shortcut_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.shortcut.received | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.sandbox.state.inserted | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file).sandbox();
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Let the scheduled parent's sandbox become active.
success (tg --url $runner.url checkpoint wait --timeout 30s runner.sandbox.state.inserted $state_watch 0 | complete) "the parent sandbox should become active"
tg --url $runner.url checkpoint continue runner.sandbox.state.inserted $state_watch 0

# The remote receives the child's sandbox connection, and the child's sandbox becomes active while the connection is held.
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sandbox.control.shortcut.received $shortcut_watch 0 | complete) "the remote should receive the child's sandbox connection"
success (tg --url $runner.url checkpoint wait --timeout 30s runner.sandbox.state.inserted $state_watch 1 | complete) "the shortcut sandbox should become active before its header"
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch

# Release the connection, and the child completes.
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.shortcut.received $shortcut_watch
let output = job recv --tag $build --timeout 30sec
success $output "the child should complete with its own sandbox and output permissions"
let read = tg --url $local.url read ($output.stdout | str trim) | complete
success $read
assert equal $read.stdout hello
