use ../lib/test.nu *

# A shortcut child's command push succeeds after the child's control connection reconnects.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
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
let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file);
		}
	',
}

# Hold the child's command push until the child's control connection has started.
let push_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.started | from json | get watch
let pushed_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.finished | from json | get watch
let started_watch = tg --url $runner.url checkpoint watch runner.process.control.start.succeeded | from json | get watch
let spawned = tg --url $local.url build --remote --detach --verbose $path | from json
let process = $spawned.process | split row '?' | first
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.command.sync.started $push_watch 0 | complete) "the child's command push should start"
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.start.succeeded $started_watch 0 | complete) "the child's control connection should start"
tg --url $runner.url checkpoint unwatch runner.process.control.start.succeeded $started_watch

# Crash and restart the remote without removing its runners, so that the child's control connection reconnects with a resume.
let pid = open --raw ($remote.directory | path join lock) | str trim | into int
kill --signal 9 $pid
wait_for_exit $pid
let remote = server start $remote

# Release the push, which must still be authorized as the child's parent.
tg --url $runner.url checkpoint unwatch runner.process.command.sync.started $push_watch
let pushed = timeout 60s tg --url $runner.url checkpoint wait runner.process.command.sync.finished $pushed_watch 0 | complete
tg --url $runner.url checkpoint unwatch runner.process.command.sync.finished $pushed_watch
success $pushed "the child's command push should succeed after its control connection reconnects"
let output = timeout 60s tg --url $local.url wait --remote $process | complete
success $output "the build should succeed"
let output = $output.stdout | from json
assert equal $output.exit 0 ($output | to json)
