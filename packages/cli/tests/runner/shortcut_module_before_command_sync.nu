use ../lib/test.nu *

# A shortcut process must load a new module while its command sync is paused.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root_token = random chars
let searches = {
	ancestor: { max_depth: 4, max_edges: 128, max_nodes: 32, page_size: 32 },
	descendant: { max_depth: 4, max_edges: 128, max_nodes: 32, page_size: 32 },
	subtree: { max_depth: 4, max_objects: 32, max_processes: 8 },
}

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	verification: { permissions: { initial: $searches, final: $searches } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let alice = tg --url $remote.url login --verbose --name alice | from json

# Give the runner room for the scheduled process and its shortcut child.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, trusted: true, url: $remote.url } },
	roles: [api indexer runner],
	verification: { permissions: { initial: $searches, final: $searches } },
	runner: { cpus: 2, id: $created.data.id, remote: default, token: $created.token.token },
}

let inserted_watch = tg --url $runner.url checkpoint watch runner.process.state.inserted | from json | get watch
let sync_watch = tg --url $runner.url checkpoint watch runner.process.command.sync.started | from json | get watch
let start_watch = tg --url $runner.url checkpoint watch runner.process.control.start.succeeded | from json | get watch

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(parent).sandbox();
		}

		export async function parent() {
			const { child } = await import("./child.tg.ts");
			return child();
		}

		export const value = 42;
	',
	child.tg.ts: '
		import { value } from "./tangram.ts";
		export function child() { return value; }
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $alice.token build $path | complete
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

# Hold command sync and require the server to process Start independently.
success (timeout 60s tg --url $runner.url checkpoint wait runner.process.command.sync.started $sync_watch 0 | complete) "the parent should reach its command sync"
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.control.start.succeeded $start_watch 0 | complete
success $started "Start must succeed while command sync is paused"
assert equal ($started.stdout | from json | get params.process) $parent
tg --url $runner.url checkpoint unwatch runner.process.control.start.succeeded $start_watch

# The imported module must execute and Finish must be indexed before command sync is released.
let outcome = timeout 30s tg --url $remote.url --token $alice.token wait --source=index $parent | from json
assert equal $outcome.exit 0
assert equal $outcome.output 42
tg --url $runner.url checkpoint unwatch runner.process.state.inserted $inserted_watch

# Release command sync and require the enclosing build to succeed.
tg --url $runner.url checkpoint unwatch runner.process.command.sync.started $sync_watch
let output = job recv --tag $build --timeout 60sec
success $output "the build should succeed"
assert equal ($output.stdout | str trim) '42'
