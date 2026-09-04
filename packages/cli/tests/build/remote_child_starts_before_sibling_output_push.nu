use ../../test.nu *

# A remote build's second child may start and complete before the runner finishes pushing the first child's output.
# The parent passes the first child's output to the second child, so the second child spawns on the runner's shortcut path. The checkpoint holds the first child's output push, and the build must still complete while it is held.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# The first child outputs a file so that the runner pushes its output. The parent and second child output strings so that only the first child pushes.
let path = artifact {
	tangram.ts: '
		export default async () => {
			let a = tg.build(child1);
			return await tg.build(child2, a);
		};
		export let child1 = () => tg.file("hello");
		export let child2 = (file: tg.File) => file.text;
	'
}

# Hold the first child's output push and watch process starts.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.output.push.started
	| from json
	| get watch
)
let start_watch = (
	tg --url $runner.url checkpoint watch runner.process.start
	| from json
	| get watch
)

let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote --user $alice.user.id $path | complete
	$output | job send --tag $job_id 0
}

# The parent and the first child start first.
for hit in [0 1] {
	let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch $hit | complete
	success $output $"process start ($hit) should be reached"
	tg --url $runner.url checkpoint continue runner.process.start $start_watch $hit
}

let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete
success $output "the first child should reach its output push"

# The second child starts while the first child's output push is held.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 2 | complete
success $output "the second child should start while the first child's output push is held"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 2
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

# The build must complete while the first child's output push is still held.
let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete while the first child's output push was held" }
}
success $output "the build should complete before the first child's output push finishes"
snapshot ($output.stdout | str trim) '"hello"'

# Release the first child's output push.
tg --url $runner.url checkpoint continue runner.process.output.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
