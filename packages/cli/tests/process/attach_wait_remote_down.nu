use ../lib/test.nu *

# A build that waits on its process through an attach fails when the remote crashes and stays down.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { multi_server: true },
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
			const file = await tg.file({ contents: "#!/bin/sh\nsleep 5\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file);
		}
	',
}

# Start the build and wait for its child to run.
let run_watch = tg --url $runner.url checkpoint watch runner.process.run | from json | get watch
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.run $run_watch 0 | complete) "the parent should run"
tg --url $runner.url checkpoint continue runner.process.run $run_watch 0
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.run $run_watch 1 | complete) "the child should run"
tg --url $runner.url checkpoint unwatch runner.process.run $run_watch

# Crash the remote and leave it down while the child runs.
let pid = open --raw ($remote.directory | path join lock) | str trim | into int
kill --signal 9 $pid

# The build fails instead of waiting for the remote forever.
let output = job recv --tag $build --timeout 60sec
failure $output "the build should fail when the remote stays down"
