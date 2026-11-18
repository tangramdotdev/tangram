use ../../util/test.nu *

# A tag get of a tag that the index has not caught up to waits for the index and then finds the tag.

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	indexer: { id: 'idx_0000000000000000000000000000' },
}
let object = tg --token $root_token put --no-tokens 'tg.file("hello")' | referent node
tg --token $root_token index --database

# Hold the database index queue, and put a tag whose index write stays in the queue.
let named = tg --token $root_token checkpoint watch indexer.database_index_queue.named_node | from json | get watch
tg --token $root_token tag put hello $object
let output = tg --token $root_token checkpoint wait --timeout 30s indexer.database_index_queue.named_node $named 0 | complete
success $output "the tag put must reach the held database index queue"

# Get the tag, and observe that it waits for the index rather than answering that the tag does not exist.
let wait = tg --token $root_token checkpoint watch index.wait.database_index_queue | from json | get watch
let get = job spawn {
	let job_id = job id
	let output = tg --token $root_token tag get hello | complete
	$output | job send --tag $job_id 0
}
let output = tg --token $root_token checkpoint wait --timeout 30s index.wait.database_index_queue $wait 0 | complete
success $output "the tag get must wait for the index"
tg --token $root_token checkpoint unwatch index.wait.database_index_queue $wait

# Release the database index queue. The tag get must find the tag.
tg --token $root_token checkpoint unwatch indexer.database_index_queue.named_node $named
let output = job recv --tag $get --timeout 30sec
success $output "the tag get should find the tag once the index catches up"
assert equal ($output.stdout | from json | get target.id) $object
