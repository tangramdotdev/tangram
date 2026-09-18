use ../../test.nu *

# A shortcut child can finish and satisfy its parent's wait while command push is blocked.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
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
let push = tg --url $runner.url checkpoint watch runner.process.command.push.started | from json | get watch
let finished = tg --url $runner.url checkpoint watch runner.process.finished | from json | get watch
let path = artifact { tangram.ts: 'export default async () => { await tg.run(await tg.file({ contents: "#!/bin/sh\nexit 0", executable: true })).sandbox(true); return 42; };' }
let run = job spawn {
	let job_id = job id
	let output = tg --url $local.url run --no-tty --remote --user $alice.user.id $path | complete
	$output | job send --tag $job_id 0
}
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.command.push.started $push 0 | complete)
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished 0 | complete)
tg --url $runner.url checkpoint continue runner.process.finished $finished 0
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished 1 | complete)
tg --url $runner.url checkpoint unwatch runner.process.finished $finished
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push
let output = job recv --tag $run --timeout 30sec
success $output
assert ($output.stdout | str contains 42)
