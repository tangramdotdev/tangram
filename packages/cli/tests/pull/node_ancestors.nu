use ../../test.nu *

# Pull supports never, missing, and always ancestor handling.

let remote = server spawn --cloud --name remote --config {
	advanced: {
		checkpoints: true,
	},
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let empty = server spawn --name empty --config {
	remotes: { default: { url: $remote.url } }
}
let coalesced = server spawn --name coalesced --config {
	remotes: { default: { url: $remote.url } }
}
let recursive = server spawn --name recursive --config {
	remotes: { default: { url: $remote.url } }
}
let scoped = server spawn --name scoped --config {
	remotes: { default: { url: $remote.url } }
}

let remote_parent = tg --url $remote.url group create parent | from json
let remote_child = tg --url $remote.url group create parent/child | from json
let remote_grandchild = tg --url $remote.url group create parent/child/grandchild | from json
tg --url $remote.url group create parent/sibling | ignore
let remote_coalesced_parent = tg --url $remote.url group create coalesced | from json
let remote_coalesced_child = tg --url $remote.url group create coalesced/child | from json
tg --url $remote.url group create coalesced/child/grandchild | ignore
let remote_coalesced_other = tg --url $remote.url group create coalesced/child/other | from json

# Never rejects a missing parent.
failure (tg --url $empty.url pull --ancestors=never parent/child | complete)

# Missing rejects a conflicting parent.
let local_parent = tg --url $local.url group create parent | from json
let local_child = tg --url $local.url group create parent/child | from json
let local_descendant = tg --url $local.url group create parent/child/local | from json
assert not equal $local_parent.id $remote_parent.id
failure (tg --url $local.url pull --ancestors=missing parent/child | complete)
assert equal (tg --url $local.url group get parent | from json | get id) $local_parent.id

# Always rejects overlapping conflicting roots and preserves their old subtree.
let output = tg --url $local.url pull --ancestors=always parent/child | complete
failure $output "always should reject a conflicting ancestor"
assert ($output.stderr | str contains "the specifier is already in use")
assert equal (tg --url $local.url group get parent | from json | get id) $local_parent.id
assert equal (tg --url $local.url group get parent/child | from json | get id) $local_child.id
assert equal (
	tg --url $local.url group get parent/child/local | from json | get id
) $local_descendant.id
success (tg --url $local.url group get $local_child.id | complete)
success (tg --url $local.url group get $local_descendant.id | complete)
success (tg --url $local.url group get $local_parent.id | complete)

# A recursive request upgrades an earlier non-recursive ancestor request without resending the node.
let database_watch = (
	tg --url $remote.url checkpoint watch sync.put.database.node --params ({
		descendants: true,
		id: $remote_coalesced_parent.id,
	} | to json)
	| from json
	| get watch
)
let ancestor_database_watch = (
	tg --url $remote.url checkpoint watch sync.put.database.node --params ({
		descendants: false,
		id: $remote_coalesced_child.id,
	} | to json)
	| from json
	| get watch
)
let ancestor_queue_watch = (
	tg --url $remote.url checkpoint watch sync.put.queue.database --params ({
		descendants: false,
		id: $remote_coalesced_child.id,
	} | to json)
	| from json
	| get watch
)
let descendants_queue_watch = (
	tg --url $remote.url checkpoint watch sync.put.queue.database --params ({
		descendants: true,
		id: $remote_coalesced_child.id,
	} | to json)
	| from json
	| get watch
)
let end_watch = (
	tg --url $remote.url checkpoint watch sync.put.input.end
	| from json
	| get watch
)
let pull = job spawn {
	let job_id = job id
	let output = (
		tg --url $coalesced.url pull --group-children coalesced/child/grandchild coalesced
		| complete
	)
	$output | job send --tag $job_id 0
}
tg --url $remote.url checkpoint wait sync.put.database.node $database_watch 0 | ignore
tg --url $remote.url checkpoint wait sync.put.queue.database $ancestor_queue_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.put.queue.database $ancestor_queue_watch 0
tg --url $remote.url checkpoint continue sync.put.database.node $database_watch 0
tg --url $remote.url checkpoint wait sync.put.queue.database $descendants_queue_watch 0 | ignore
tg --url $remote.url checkpoint wait sync.put.database.node $ancestor_database_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.put.database.node $ancestor_database_watch 0
tg --url $remote.url checkpoint wait sync.put.input.end $end_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.put.input.end $end_watch 0
tg --url $remote.url checkpoint continue sync.put.queue.database $descendants_queue_watch 0
tg --url $remote.url checkpoint unwatch sync.put.database.node $ancestor_database_watch
tg --url $remote.url checkpoint unwatch sync.put.database.node $database_watch
tg --url $remote.url checkpoint unwatch sync.put.input.end $end_watch
tg --url $remote.url checkpoint unwatch sync.put.queue.database $ancestor_queue_watch
tg --url $remote.url checkpoint unwatch sync.put.queue.database $descendants_queue_watch
success (job recv --tag $pull --timeout 10sec)
assert equal (
	tg --url $coalesced.url group get --local coalesced/child/other | from json | get id
) $remote_coalesced_other.id

# Pulling children rejects a conflicting descendant when the requested root already matches.
tg --url $recursive.url pull parent
let local_child = tg --url $recursive.url group create parent/child | from json
assert not equal $local_child.id $remote_child.id
let output = tg --url $recursive.url pull --group-children parent | complete
failure $output "pulling children should reject a conflicting descendant"
assert ($output.stderr | str contains "the specifier is already in use")
assert equal (tg --url $recursive.url group get parent/child | from json | get id) $local_child.id

# Pulling a subtree does not pull siblings through a dynamically requested ancestor.
tg --url $scoped.url pull --group-children parent/child
failure (tg --url $scoped.url group get --local parent/sibling | complete)
