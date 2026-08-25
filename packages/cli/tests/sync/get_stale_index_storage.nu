use ../../test.nu *

# A pull computes whether each object's subtree is fully stored from the children as they arrive, and
# the local index reports the same thing, but can be behind: a plain get stores a single object and
# indexes it with `storage.subtree` false. A stale false must not replace a computed true, because the
# computed value is recomputed only when a child changes, and every child has already arrived. If it
# did, the root would never count as stored, and the pull would never finish, even though every
# object arrived and nothing failed.

let remote = server spawn --cloud --name remote
let client = server spawn --name client --config {
	advanced: { checkpoints: true },
	remotes: { default: { url: $remote.url } },
}

# Put a directory with two branches, a file and a deeper directory. The remote sends a level at
# a time, so the deeper branch's blob is the last object to arrive, and the root is still incomplete
# when the file's own subtree is complete.
let directory = (
	tg --url $remote.url put 'tg.directory({
		"f": tg.file("fff"),
		"z": tg.directory({ "v": tg.file("vvv") }),
	})'
	| str trim
)

# Name the file and both branches' blobs. Objects are content addressed, so putting the same
# contents again yields the ids of the objects that are already in the directory.
let file = tg --url $remote.url put 'tg.file("fff")' | str trim
let file_blob = tg --url $remote.url put 'tg.blob("fff")' | str trim
let deep_blob = tg --url $remote.url put 'tg.blob("vvv")' | str trim
tg --url $remote.url index

# Get the file. This stores the file's own data but not the blob it points to, and it writes the
# index entry that the pull later reads.
tg --url $client.url get $file | ignore
tg --url $client.url index
assert equal (tg --url $client.url availability --local $file | from json) {} "the index must report that the file's subtree is unavailable"

# Hold the file's blob so that the index task selects the stale lookup before the graph computes the
# file as available.
let file_blob_input_watch = (
	tg --url $client.url checkpoint watch sync.get.input.object --params ({ id: $file_blob } | to json)
	| from json
	| get watch
)

# Hold the last object to arrive so that the root cannot finish before the stale value lands.
let deep_blob_input_watch = (
	tg --url $client.url checkpoint watch sync.get.input.object --params ({ id: $deep_blob } | to json)
	| from json
	| get watch
)

# Hold the index task immediately before it checks the graph.
let filter_watch = (
	tg --url $client.url checkpoint watch sync.get.index.object.filter --params ({ id: $file } | to json)
	| from json
	| get watch
)

# Hold the stale index request until the file has become available in the graph.
let index_watch = (
	tg --url $client.url checkpoint watch sync.get.index.object --params ({ id: $file } | to json)
	| from json
	| get watch
)

let pull = job spawn {
	let job_id = job id
	let output = tg --url $client.url pull $directory | complete
	$output | job send --tag $job_id 0
}

# Wait until the file's blob and the index task are both held, then let the index task select the
# lookup while the file is not yet available.
tg --url $client.url checkpoint wait sync.get.input.object $file_blob_input_watch 0 | ignore
tg --url $client.url checkpoint wait sync.get.index.object.filter $filter_watch 0 | ignore
tg --url $client.url checkpoint continue sync.get.index.object.filter $filter_watch 0
tg --url $client.url checkpoint unwatch sync.get.index.object.filter $filter_watch
tg --url $client.url checkpoint wait sync.get.index.object $index_watch 0 | ignore

# Let the file's blob arrive, then wait until every object except the deeper branch's blob has
# arrived. The graph has now computed that the file is available.
tg --url $client.url checkpoint continue sync.get.input.object $file_blob_input_watch 0
tg --url $client.url checkpoint unwatch sync.get.input.object $file_blob_input_watch
tg --url $client.url checkpoint wait sync.get.input.object $deep_blob_input_watch 0 | ignore

# Let the index task write the stale value over the computed one.
tg --url $client.url checkpoint continue sync.get.index.object $index_watch 0
tg --url $client.url checkpoint unwatch sync.get.index.object $index_watch

# Release the last object. Nothing is missing and nothing errors.
tg --url $client.url checkpoint continue sync.get.input.object $deep_blob_input_watch 0
tg --url $client.url checkpoint unwatch sync.get.input.object $deep_blob_input_watch

let output = job recv --tag $pull --timeout 10sec
success $output "the pull must end once every object has been transferred"
