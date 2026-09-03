use ../../test.nu *

# The children of a process running on a remote runner are readable through the remote API server while its indexed children list is incomplete.

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
			await tg.build(a);
			await tg.build(b);
			await tg.build(c);
			return "parent";
		}
		export function a() { return "a"; }
		export function b() { return "b"; }
		export function c() { return "c"; }
	',
}

# Hold every process finish on the remote.
let finish_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.finish
	| from json
	| get watch
)
let process = tg --url $local.url build --detach --remote $path | str trim

# Hold the first child's finish so that the parent keeps running while its indexed children list is incomplete.
let first_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 0 | from json
let first = $first_hit.params.id
assert ($first != $process) "the first finish should belong to a child"

# Start a paginated stream that must receive the later children from the runner.
let response_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.response.publish --params '{"kind":"get_children"}'
	| from json
	| get watch
)
let stream_job = job spawn {
	let job_id = job id
	let output = tg --url $local.url process children --length 3 --no-timeout --size 1 $process | complete
	$output | job send --tag $job_id 0
}

# Let the stream read the first child and reach the current end of the list.
tg --url $remote.url --token $root_token checkpoint wait process.control.response.publish $response_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint continue process.control.response.publish $response_watch 0
tg --url $remote.url --token $root_token checkpoint wait process.control.response.publish $response_watch 1 | ignore
tg --url $remote.url --token $root_token checkpoint continue process.control.response.publish $response_watch 1
tg --url $remote.url --token $root_token checkpoint unwatch process.control.response.publish $response_watch

# Let the parent spawn the remaining children, holding each finish in turn.
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 0
let second_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 1 | from json
let second = $second_hit.params.id
assert ($second != $process) "the second finish should belong to a child"
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 1
let third_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 2 | from json
let third = $third_hit.params.id
assert ($third != $process) "the third finish should belong to a child"

# The stream must wake for each child and preserve their spawn order across pages.
let output = job recv --tag $stream_job --timeout 30sec
success $output "the children stream should receive all running remote children"
let children = $output.stdout | from json
let listed = $children | each { get process | split row '?' | first }
assert equal $listed [$first $second $third] "the stream should preserve the child spawn order"

# Snapshot pagination and end-relative positions must also be served by the runner.
let middle = tg --url $local.url process children --length 1 --position 1 $process | from json
let middle = $middle | first | get process | split row '?' | first
assert equal $middle $second "the middle child should be readable by position"
let tail = tg --url $local.url process children --position=end.-2 --size 1 $process | from json
let tail = $tail | each { get process | split row '?' | first }
assert equal $tail [$second $third] "the tail should be readable relative to the end"

# Release the remaining finishes and confirm the build completes.
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 2
let parent_hit = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $finish_watch 3 | from json
assert equal $parent_hit.params.id $process "the fourth finish should belong to the parent"
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $finish_watch 3
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $finish_watch
let output = tg --url $local.url wait $process | complete
success $output "the parent should finish after its finish checkpoint continues"
