use ../../test.nu *

# A remote runner must not push a process's output before its command push completes.

let root_token = random chars

# Spawn the remote. It holds no runner role, so the build can only complete by way of the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [cleaner http indexer scheduler],
}

# Create the runner and its token.
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [http indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Watch the command push so it can be held.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.command.push.started
	| from json
	| get watch
)

# The child spawn takes the runner shortcut, so the runner must push its command.
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

# Hold the command push.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 0 | complete
success $output "the runner must push the child command on the shortcut path"

# The build must not complete while the command push is held, because the output push must wait for it.
let held = try { job recv --tag $build --timeout 5sec } catch { null }
if $held != null {
	error make { msg: "the build completed while the command push was held" }
}

# Release the command push.
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

# The build must complete and the user must read the output.
let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete after the command push was released" }
}
success $output "the build must succeed after the command push completes"

let file = $output.stdout | str trim
let read = tg --url $local.url get $file --depth inf | complete
success $read "the user must read the pushed output"
