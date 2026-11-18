use ../../util/test.nu *

# A tag get of a committed tag that the index has not caught up to finds the tag.

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

# Get the tag while the index lags, and observe whether the tag get waits for the index, so that the queue is released only after the tag get either finishes or waits.
let wait = tg --token $root_token checkpoint watch index.wait.database_index_queue | from json | get watch
job spawn {
	let output = tg --token $root_token tag get hello | complete
	{ kind: get, output: $output } | job send 0
}
job spawn {
	tg --token $root_token checkpoint wait --timeout 30s index.wait.database_index_queue $wait 0 | complete | ignore
	{ kind: wait } | job send 0
}
let message = job recv --timeout 30sec

# Release the database index queue.
tg --token $root_token checkpoint unwatch index.wait.database_index_queue $wait
tg --token $root_token checkpoint unwatch indexer.database_index_queue.named_node $named

# The tag get must find the tag.
let message = if $message.kind == get { $message } else { job recv --timeout 30sec }
success $message.output "the tag get should find the committed tag"
assert equal ($message.output.stdout | from json | get target.id) $object
