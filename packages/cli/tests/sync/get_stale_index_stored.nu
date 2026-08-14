use ../../test.nu *

# A pull computes whether each object's subtree is fully stored from the children as they arrive, and
# the local index reports the same thing, but can be behind: a plain get stores a single object and
# indexes it with `stored.subtree` false. A stale false must not replace a computed true, because the
# computed value is recomputed only when a child changes, and every child has already arrived. If it
# did, the root would never count as stored, and the pull would never finish, even though every
# object arrived and nothing failed.

let remote = spawn --cloud --name remote
let client = spawn --name client --config {
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

# Name the file and the deeper branch's blob. Objects are content addressed, so putting the same
# contents again yields the id of the object that is already in the directory.
let file = tg --url $remote.url put 'tg.file("fff")' | str trim
let deep_blob = tg --url $remote.url put 'tg.blob("vvv")' | str trim
tg --url $remote.url index

# Get the file. This stores the file's own data but not the blob it points to, and it writes the
# index entry that the pull later reads.
tg --url $client.url get $file | ignore
tg --url $client.url index
assert equal (tg --url $client.url stored --local $file | from json) {} "the index must report that the file's subtree is not stored"

# Hold the last object to arrive, so that the root cannot finish before the stale value lands.
let input_watch = (
	tg --url $client.url checkpoint watch sync.get.input.object --params ({ id: $deep_blob } | to json)
	| from json
	| get watch
)

# Hold the file's visit to the index task until its own blob has arrived.
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

# Every object except the held one has arrived, so the pull has computed that the file is stored.
tg --url $client.url checkpoint wait sync.get.input.object $input_watch 0 | ignore

# Let the index task write the stale value over the computed one.
tg --url $client.url checkpoint wait sync.get.index.object $index_watch 0 | ignore
tg --url $client.url checkpoint continue sync.get.index.object $index_watch 0
tg --url $client.url checkpoint unwatch sync.get.index.object $index_watch

# Release the last object. Nothing is missing and nothing errors.
tg --url $client.url checkpoint continue sync.get.input.object $input_watch 0
tg --url $client.url checkpoint unwatch sync.get.input.object $input_watch

let output = job recv --tag $pull --timeout 10sec
success $output "the pull must end once every object has been transferred"
