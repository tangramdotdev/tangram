use ../../test.nu *

# A push can transfer an object with a child the source does not have, when the source knows the
# child exists on the remote and is authorized to use it, as with a cache hit. The source replies
# missing for the child, and the remote falls back to its own index, but the child's index write is
# asynchronous, so the index can be behind the store. The remote must force the index to catch up
# and retry the touch instead of failing the push.

# Create a remote server with checkpoints enabled.
let remote = spawn --cloud --name remote --config { advanced: { checkpoints: true } }

# Create a local server.
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

# Hold the remote's index batches, so that its store receives the child while its index does not.
let batch_watch = (
	tg --url $remote.url checkpoint watch index.batch
	| from json
	| get watch
)

# Put the child and the parent on the remote. The puts return once the store writes complete, while
# the index writes are held.
let file = tg --url $remote.url put 'tg.file("hello")' | str trim
let directory = tg --url $remote.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

# Put the parent on the local server without the child, as a cache hit would.
tg --url $remote.url get --bytes $directory | tg --url $local.url put --bytes --kind dir

# Hold the remote's sync get immediately before it touches the child, so that the touch is
# guaranteed to run while the index is behind.
let index_watch = (
	tg --url $remote.url checkpoint watch sync.get.index.object --params ({ id: $file } | to json)
	| from json
	| get watch
)

# Push the parent. The source reports the child missing, and the remote falls back to its index.
let push = job spawn {
	let job_id = job id
	let output = tg --url $local.url push $directory | complete
	$output | job send --tag $job_id 0
}

# Wait for the sync get to reach the touch, then let the touch run against the stale index.
tg --url $remote.url checkpoint wait sync.get.index.object $index_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.get.index.object $index_watch 0
tg --url $remote.url checkpoint unwatch sync.get.index.object $index_watch

# Release the held batches so the index can catch up.
tg --url $remote.url checkpoint unwatch index.batch $batch_watch

let output = job recv --tag $push --timeout 30sec
success $output "the push must succeed once the index catches up"
