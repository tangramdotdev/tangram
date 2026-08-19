use ../../test.nu *

# An incomplete local children list falls through to a remote instead of appearing empty.

let remote = spawn --cloud --name remote
let source = spawn --name source --config {
	remotes: { default: { url: $remote.url } },
}
let local = spawn --name local --config {
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

# Pause a pull after storing the process data but before completing its children list.
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

failure (tg process children --local $process | complete) "incomplete local children should not appear empty"

# The default location order should fall through to the remote.
let children = tg process children $process | from json
let remote_child = $remote_children | first | get process
let child = $children | first | get process
assert equal ($child | split row '?' | first) ($remote_child | split row '?' | first) "the child process should be read from the remote"
assert ($remote_child | str contains '?location=local') "the remote should describe its child as local"
assert ($child | str contains '?location=remote') "the local server should describe the remote child as remote"

# Complete the pull and read the authoritative local children list.
tg checkpoint continue sync.get.store.process $watch 0
tg checkpoint unwatch sync.get.store.process $watch
success (job recv --tag $pull --timeout 10sec)
let local_children = tg process children --local $process | from json
assert equal $local_children $remote_children "the complete local children should be readable"
