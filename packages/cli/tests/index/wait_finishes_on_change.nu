use ../lib/test.nu *

# A wait that finds an index write pending must finish as soon as that write lands, without waiting for the poll interval.
let server = server spawn --config {
	advanced: { checkpoints: true },
	indexer: { request: { poll_interval: 1 } },
}

# Hold the index write of a put so the wait's first pass finds it pending.
let batch_watch = tg --url $server.url checkpoint watch index.batch | from json | get watch
let wait_watch = tg --url $server.url checkpoint watch indexer.request.wait | from json | get watch
tg --url $server.url put -k directory 'tg.directory({})' | ignore
let request = job spawn {
	let job_id = job id
	let output = tg --url $server.url index | complete
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $server.url checkpoint wait index.batch $batch_watch 0 | ignore
timeout 10s tg --url $server.url checkpoint wait indexer.request.wait $wait_watch 0 | ignore
tg --url $server.url checkpoint continue indexer.request.wait $wait_watch 0
tg --url $server.url checkpoint unwatch indexer.request.wait $wait_watch

# The wait is pending on the held write.
let early = try { job recv --tag $request --timeout 200ms } catch { null }
assert ($early == null) "the wait should be pending on the held index write"

# Release the write: the wait must finish well within one poll interval.
let start = date now
tg --url $server.url checkpoint continue index.batch $batch_watch 0
tg --url $server.url checkpoint unwatch index.batch $batch_watch
let output = job recv --tag $request --timeout 10sec
let elapsed = (date now) - $start
success $output
assert ($elapsed < 500ms) $"the wait should finish without a poll interval tick, took ($elapsed)"
