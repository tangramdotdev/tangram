use ../lib/test.nu *

# A remote runner can return a token-bearing output before its command push completes.

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
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
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

# The runner-backed wait can return the output while the command push is held.
let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete while the command push was held" }
}
success $output "the build must return its output before the command push completes"
let file = $output.stdout | str trim
let params = $'http://localhost/($file)' | url parse | get params
assert ($params | where {|param| $param.key starts-with 'tokens[' } | any {|param|
	let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	($body.resource | str starts-with 'syn_') and ('sync_read' in $body.permissions)
}) "the output must carry sync authorization"

# Release the command push.
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

# The returned referent must allow the user to read the output.
let read = tg --url $local.url cat $file | complete
success $read "the user must read the returned output"
assert equal $read.stdout 'hello'
