use ../../test.nu *

# Concurrent requests to await indexing share the indexer and all complete.

let server = server spawn --config {
	advanced: {
		checkpoints: true,
	},
	indexer: {
		request: { poll_interval: 0.01 },
	},
}
let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	'
}
let id = tg --url $server.url checkin $path

def index_background [url: string] {
	job spawn {
		let job_id = job id
		let output = tg --url $url index | complete
		$output | job send --tag $job_id 0
	}
}

let wait_watch = (
	tg --url $server.url checkpoint watch indexer.request.wait
	| from json
	| get watch
)

# Hold the first request while it waits for tasks.
let first = index_background $server.url
tg --url $server.url checkpoint wait indexer.request.wait $wait_watch 0 | ignore

# Start a second request and hold it after the indexer records it. At this point
# both requests are live in the indexer.
let receive_watch = (
	tg --url $server.url checkpoint watch indexer.request.receive
	| from json
	| get watch
)
let second = index_background $server.url
tg --url $server.url checkpoint wait indexer.request.receive $receive_watch 0 | ignore

tg --url $server.url checkpoint continue indexer.request.receive $receive_watch 0
tg --url $server.url checkpoint unwatch indexer.request.receive $receive_watch
tg --url $server.url checkpoint continue indexer.request.wait $wait_watch 0
tg --url $server.url checkpoint unwatch indexer.request.wait $wait_watch

for index in [$first $second] {
	let output = job recv --tag $index --timeout 10sec
	success $output
}

let metadata = tg --url $server.url object metadata $id | from json
assert ($metadata.subtree.count > 0)
