use ../../test.nu *

# A named node remains pending while its ancestor requests are registered.

let remote = spawn --cloud --name remote --config {
	advanced: {
		checkpoints: true,
	},
}
let local = spawn --name local --config {
	advanced: {
		checkpoints: true,
	},
	remotes: { default: { url: $remote.url } },
}

let parent = tg --url $remote.url group create ancestor | from json
let child = tg --url $remote.url group create ancestor/child | from json
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("trigger"); }',
}
let process = tg --url $remote.url build --detach $path | str trim
tg --url $remote.url wait $process

let send_watch = (
	tg --url $remote.url checkpoint watch sync.put.database.node.send --params ({
		id: $child.id,
	} | to json)
	| from json
	| get watch
)
let store_watch = (
	tg --url $local.url checkpoint watch sync.get.store.process --params ({ id: $process } | to json)
	| from json
	| get watch
)
let store_end_watch = (
	tg --url $local.url checkpoint watch sync.get.store.process.end --params ({
		id: $process,
	} | to json)
	| from json
	| get watch
)
let ancestor_watch = (
	tg --url $local.url checkpoint watch sync.get.input.node.ancestor --params ({
		id: $child.id,
	} | to json)
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = tg --url $local.url pull $process ancestor/child | complete
	$output | job send --tag $job_id 0
}
tg --url $remote.url checkpoint wait sync.put.database.node.send $send_watch 0 | ignore
tg --url $local.url checkpoint wait sync.get.store.process $store_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.put.database.node.send $send_watch 0
tg --url $local.url checkpoint wait sync.get.input.node.ancestor $ancestor_watch 0 | ignore
tg --url $local.url checkpoint continue sync.get.store.process $store_watch 0
let store_end_hit = (
	tg --url $local.url checkpoint wait sync.get.store.process.end $store_end_watch 0
	| from json
)
tg --url $local.url checkpoint continue sync.get.store.process.end $store_end_watch 0
tg --url $local.url checkpoint continue sync.get.input.node.ancestor $ancestor_watch 0
tg --url $local.url checkpoint unwatch sync.get.input.node.ancestor $ancestor_watch
tg --url $local.url checkpoint unwatch sync.get.store.process $store_watch
tg --url $local.url checkpoint unwatch sync.get.store.process.end $store_end_watch
tg --url $remote.url checkpoint unwatch sync.put.database.node.send $send_watch
success (job recv --tag $pull --timeout 10sec)
assert not $store_end_hit.params.end
assert equal (tg --url $local.url group get --local ancestor | from json | get id) $parent.id
