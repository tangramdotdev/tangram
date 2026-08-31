use ../../test.nu *

# A sibling's cache-hit query and module loads must stay correct while every command push is delayed.

let root_token = random chars

# Spawn the remote. It holds no runner role, so the build can only complete by way of the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Watch the command pushes so they can be held.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.command.push.started
	| from json
	| get watch
)

# tangram.ts and dep.tg.ts import each other. foo and bar both build dep, so one of them receives
# it as a cache hit while dep's command push is still held.
let path = artifact {
	tangram.ts: '
		import dep from "./dep.tg.ts";
		import file from "./file.txt";

		export default () => {
			return tg.directory({
				foo: tg.build(foo),
				bar: tg.build(bar),
			})
		}

		export const foo = () => {
			return tg.build(dep);
		};

		export const bar = () => {
			return tg.build(dep);
		};
	',
	dep.tg.ts: '
		import * as root from "./tangram.ts";
		import file from "./file.txt";
		export default async () => {
			await tg.sleep(5);
			return file;
		};
	',
	file.txt: 'hello, world asdfad',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Hold every command push across dep's build window, then release them all by unwatching.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 0 | complete
success $output "the runner must push a child command on the shortcut path"
sleep 8sec
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

# The build must complete and the user must read the output.
let result = try { job recv --tag $build --timeout 60sec } catch { null }
if $result == null {
	error make { msg: "the build did not complete after the command pushes were released" }
}
success $result "the build must succeed with delayed command pushes"

let directory = $result.stdout | str trim
let read = tg --url $local.url get $directory --depth inf | complete
success $read "the user must read the pushed output"
