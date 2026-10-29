use ../../lib/test.nu *

# A wait that finds an index write pending must finish as soon as that write lands, without waiting for the poll interval.

# Use a poll interval longer than the test's timeout, so that the wait can only finish when it is notified of the write.
let local = server spawn --config {
	advanced: { checkpoints: true },
	indexer: { request: { poll_interval: 3600 } },
}

# Hold the index write of a put so the wait's first pass finds it pending.
let batch_watch = tg --url $local.url checkpoint watch index.batch | from json | get watch
let wait_watch = tg --url $local.url checkpoint watch indexer.request.wait | from json | get watch
tg --url $local.url put --no-tokens -k directory 'tg.directory({})' | ignore
let request = job spawn {
	let job_id = job id
	let output = tg --url $local.url index | complete
	$output | job send --tag $job_id 0
}
tg --url $local.url checkpoint wait --timeout 10s index.batch $batch_watch 0 | ignore
tg --url $local.url checkpoint wait --timeout 10s indexer.request.wait $wait_watch 0 | ignore
tg --url $local.url checkpoint continue indexer.request.wait $wait_watch 0
tg --url $local.url checkpoint unwatch indexer.request.wait $wait_watch

# The wait is pending on the held write.
let early = try { job recv --tag $request --timeout 0sec } catch { null }
assert ($early == null) "the wait should be pending on the held index write"

# Release the write. The wait must finish without a poll interval tick.
tg --url $local.url checkpoint continue index.batch $batch_watch 0
tg --url $local.url checkpoint unwatch index.batch $batch_watch
let output = job recv --tag $request --timeout 30sec
success $output
