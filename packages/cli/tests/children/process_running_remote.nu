use ../../test.nu *

# The children of a process running on a remote runner are readable through the remote API server before the process finishes and its children are indexed.

let root_token = random chars

# Spawn the remote with checkpoints enabled and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner as a separate server so that the remote's runner state is empty.
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child);
			return "parent";
		}
		export function child() { return "child"; }
	',
}

# Hold every process finish on the remote.
let finish_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.finish
	| from json
	| get watch
)
let process = tg --url $local.url build --detach --remote $path | str trim

# Hold the child's finish so that the parent keeps running and its children are not indexed on the remote.
let child_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 0 | from json
let child = $child_hit.params.id
assert ($child != $process) "the first finish should belong to the child"

# The parent's children must be served from the runner while the parent is running.
let output = tg --url $local.url process children $process | complete
success $output "the children of a running remote process should be readable"
let children = $output.stdout | from json
assert equal ($children | length) 1 "the parent should have one child"
let listed = $children | first | get process | split row '?' | first
assert equal $listed $child "the listed child should be the running child process"

# Release the finishes and confirm the build completes.
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 0
let parent_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 1 | from json
assert equal $parent_hit.params.id $process "the second finish should belong to the parent"
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 1
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $finish_watch
let output = tg --url $local.url wait $process | complete
success $output "the parent should finish after its finish checkpoint continues"
