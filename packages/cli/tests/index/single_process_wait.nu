use ../../test.nu *

# The memory messenger must keep an acknowledged request alive past its acknowledgment timeout.
let server = server spawn --config {
	advanced: { checkpoints: true, single_process: true },
	indexer: { request: { timeout: 0.05 } },
}
let url = $server.url
let receive_watch = tg --url $url checkpoint watch indexer.request.receive | from json | get watch
let wait_watch = tg --url $url checkpoint watch indexer.request.wait | from json | get watch
let request = job spawn {
	let id = job id
	let output = tg --url $url index | complete
	$output | job send --tag $id 0
}

tg --url $url checkpoint wait indexer.request.receive $receive_watch 0 | ignore
tg --url $url checkpoint continue indexer.request.receive $receive_watch 0
tg --url $url checkpoint wait indexer.request.wait $wait_watch 0 | ignore
sleep 1500ms
tg --url $url checkpoint unwatch indexer.request.wait $wait_watch

# Any replacement request would be held at the next receive checkpoint.
let output = job recv --tag $request --timeout 10sec
success $output "the original request through the memory messenger must finish"
tg --url $url checkpoint unwatch indexer.request.receive $receive_watch
