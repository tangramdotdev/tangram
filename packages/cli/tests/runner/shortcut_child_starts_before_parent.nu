use ../lib/test.nu *

# A shortcut child whose command push and start reach the remote before its shortcut parent's must still be recorded as the parent's child with access to its command.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Give the runner room for the scheduled grandparent, the shortcut parent, and the shortcut child.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { cpus: 3, id: $created.data.id, remote: default, token: $created.token.token },
}

let inserted_watch = tg --url $runner.url checkpoint watch runner.process.state.inserted | from json | get watch
let push_watch = tg --url $runner.url checkpoint watch runner.process.command.push.started | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished | from json | get watch

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(parent).sandbox();
		}

		export async function parent() {
			return await tg.build(child).sandbox();
		}

		export function child() {
			return 42;
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token build $path | complete
	$output | job send --tag $job_id 0
}

# Record the scheduled grandparent and the shortcut parent.
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.state.inserted $inserted_watch 0 | complete
success $output "the grandparent should enter the runner state"
tg --url $runner.url checkpoint continue runner.process.state.inserted $inserted_watch 0
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.state.inserted $inserted_watch 1 | complete
success $output "the parent should enter the runner state"
let parent = $output.stdout | from json | get params.process
tg --url $runner.url checkpoint continue runner.process.state.inserted $inserted_watch 1

# Hold the parent's command push so the remote does not start the parent.
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 0 | complete) "the parent should reach its command push"

# Let the child's push through so its start can reach the remote first.
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.state.inserted $inserted_watch 2 | complete
success $output "the child should enter the runner state"
let child = $output.stdout | from json | get params.process
tg --url $runner.url checkpoint continue runner.process.state.inserted $inserted_watch 2
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 1 | complete) "the child should reach its command push"
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 1

# The child runs to completion while the parent's start is held.
let finished = timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete
if $finished.exit_code == 0 {
	assert equal ($finished.stdout | from json | get params.process) $child "the child should finish first"
	tg --url $runner.url checkpoint continue runner.process.finished $finished_watch 0
}
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch
tg --url $runner.url checkpoint unwatch runner.process.state.inserted $inserted_watch

# Release the parent's push.
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

let output = job recv --tag $build --timeout 60sec
success $output "the build should succeed"
assert equal ($output.stdout | str trim) '42'

let output = timeout 60s tg --url $remote.url --token $root_token wait $parent | complete
success $output "the parent should finish on the remote"
let outcome = $output.stdout | from json
assert equal $outcome.exit 0 "the parent should succeed"
let children = tg --url $remote.url --token $root_token process children --no-tokens $parent | from json
assert equal ($children | length) 1 "the remote should record the child under the parent"
assert equal ($children | get 0.process | split row '?' | first) $child
