use ../../test.nu *

# A process's complete direct children list is available before its child processes are pulled.

let remote = server spawn --cloud --name remote
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
}
tg remote put default $remote.url

let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child);
			return "parent";
		}
		export function child() { return "child"; }
	',
}
let process = tg --url $source.url build --detach $path | str trim
tg --url $source.url wait $process
tg --url $source.url push --process-children $process
tg --url $remote.url wait $process
let remote_children = tg --url $remote.url process children --local $process | from json
assert equal ($remote_children | length) 1 "the remote process should have a child"

# Pause a pull after storing the process data but before the sync finishes.
let watch = (
	tg checkpoint watch sync.get.store.process --params ({ id: $process } | to json)
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = tg pull $process | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait sync.get.store.process $watch 0 | ignore

# The direct children list is authoritative even though the child process was not pulled.
let children = tg process children --local $process | from json
let remote_child = $remote_children | first | get process
let child = $children | first | get process
let child_id = $child | split row '?' | first
assert equal $child_id ($remote_child | split row '?' | first) "the local children list should match the remote"
assert ($remote_child | str contains '?location=local') "the remote should describe its child as local"
assert ($child | str contains '?location=local') "the local server should describe its authoritative child referent as local"
failure (tg process get --local $child_id | complete) "the child process should not be present locally"
success (tg process get $child_id | complete) "the child process should still be readable from the remote"

# Complete the pull and confirm the direct children list remains available locally.
tg checkpoint continue sync.get.store.process $watch 0
tg checkpoint unwatch sync.get.store.process $watch
success (job recv --tag $pull --timeout 10sec)
let local_children = tg process children --local $process | from json
assert equal $local_children $remote_children "the complete local children should be readable"
