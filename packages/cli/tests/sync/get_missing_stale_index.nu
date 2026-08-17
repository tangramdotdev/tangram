use ../../test.nu *

# Ensure a push succeeds when the source is missing a child which is in the remote's store but not yet in its index.

let remote = spawn --cloud --name remote --config { advanced: { checkpoints: true } }
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

# Hold the remote's index writes.
let batch_watch = (
	tg --url $remote.url checkpoint watch index.batch
	| from json
	| get watch
)

# Put the child and the parent on the remote.
let file = tg --url $remote.url put 'tg.file("hello")' | str trim
let directory = tg --url $remote.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim

# Put the parent on the local server without the child.
tg --url $remote.url get --bytes $directory | tg --url $local.url put --bytes --kind dir

# Hold the remote's sync get immediately before it touches the child.
let index_watch = (
	tg --url $remote.url checkpoint watch sync.get.index.object --params ({ id: $file } | to json)
	| from json
	| get watch
)

# Push the parent.
let push = job spawn {
	let job_id = job id
	let output = tg --url $local.url push $directory | complete
	$output | job send --tag $job_id 0
}

# Let the touch run against the stale index, then release the index writes.
tg --url $remote.url checkpoint wait sync.get.index.object $index_watch 0 | ignore
tg --url $remote.url checkpoint continue sync.get.index.object $index_watch 0
tg --url $remote.url checkpoint unwatch sync.get.index.object $index_watch
tg --url $remote.url checkpoint unwatch index.batch $batch_watch

let output = job recv --tag $push --timeout 30sec
success $output
