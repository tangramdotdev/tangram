use ../../../test.nu *

# A tag mutation racing a checkout removes the stale entry after the checkout releases the lock.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
}
let first = artifact 'first'
let second = artifact 'second'
let tag_path = $server.directory | path join store dep

tg tag dep $first

# Race a replacement against a checkout holding the checkout lock.
let checkout_watch = (
	tg checkpoint watch checkout.named.materialize
	| from json
	| get watch
)
let indexer_watch = (
	tg checkpoint watch indexer.database_outbox.named_node
	| from json
	| get watch
)
let checkout = job spawn {
	let job_id = job id
	let output = tg checkout dep | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkout.named.materialize $checkout_watch 0 | ignore
let replacement = job spawn {
	let job_id = job id
	let output = tg tag --force dep $second | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait indexer.database_outbox.named_node $indexer_watch 0 | ignore
tg checkpoint continue indexer.database_outbox.named_node $indexer_watch 0
tg checkpoint unwatch indexer.database_outbox.named_node $indexer_watch
tg checkpoint continue checkout.named.materialize $checkout_watch 0
tg checkpoint unwatch checkout.named.materialize $checkout_watch
success (job recv --tag $checkout --timeout 10sec)
success (job recv --tag $replacement --timeout 10sec)
assert (not ($tag_path | path exists --no-symlink)) "expected replacement to remove the stale checkout"

let path = tg checkout dep | str trim
assert equal (open $path) 'second' "expected checkout to use the replacement target"

# Race a deletion against a checkout holding the checkout lock.
let checkout_watch = (
	tg checkpoint watch checkout.named.materialize
	| from json
	| get watch
)
let indexer_watch = (
	tg checkpoint watch indexer.database_outbox.named_node
	| from json
	| get watch
)
let checkout = job spawn {
	let job_id = job id
	let output = tg checkout dep | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait checkout.named.materialize $checkout_watch 0 | ignore
let deletion = job spawn {
	let job_id = job id
	let output = tg tag delete dep | complete
	$output | job send --tag $job_id 0
}
tg checkpoint wait indexer.database_outbox.named_node $indexer_watch 0 | ignore
tg checkpoint continue indexer.database_outbox.named_node $indexer_watch 0
tg checkpoint unwatch indexer.database_outbox.named_node $indexer_watch
tg checkpoint continue checkout.named.materialize $checkout_watch 0
tg checkpoint unwatch checkout.named.materialize $checkout_watch
success (job recv --tag $checkout --timeout 10sec)
success (job recv --tag $deletion --timeout 10sec)
assert (not ($tag_path | path exists --no-symlink)) "expected deletion to remove the stale checkout"
