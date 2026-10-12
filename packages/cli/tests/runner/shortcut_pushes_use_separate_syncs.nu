use ../lib/test.nu *

# The runner pushes a shortcut child's command and its outcome in separate syncs, so that a finished command push never answers for the outcome objects.

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
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let command_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.started | from json | get watch
let outcome_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.build(child);
		};

		export const child = () => {
			return tg.file("hello");
		};
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Record the sync of the child's command push and of its outcome push, which is the first outcome push because the child finishes before its parent.
let command = timeout 30s tg --url $runner.url checkpoint wait runner.process.command.sync.started $command_watch 0 | from json
tg --url $runner.url checkpoint continue runner.process.command.sync.started $command_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.sync.started $command_watch
let outcome = timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $outcome_watch 0 | from json
tg --url $runner.url checkpoint continue runner.process.outcome.sync.started $outcome_watch 0
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $outcome_watch
assert ($command.params.sync != $outcome.params.sync) "the command and outcome pushes must use separate syncs"

# The build's output is readable.
let output = job recv --tag $build --timeout 30sec
success $output
let read = tg --url $local.url cat ($output.stdout | str trim) | complete
success $read "the user must read the returned output"
assert equal $read.stdout 'hello'
