use ../../test.nu *

# Concurrent requests share the server waiter and keep their own cutoffs.

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

# Queue a later request while the first batch is waiting for local tasks.
let receive_watch = (
	tg --url $server.url checkpoint watch indexer.request.receive
	| from json
	| get watch
)
let second = index_background $server.url
tg --url $server.url checkpoint continue indexer.request.wait $wait_watch 0

# The later request starts its own local wait after the first batch finishes.
tg --url $server.url checkpoint wait indexer.request.receive $receive_watch 0 | ignore

tg --url $server.url checkpoint continue indexer.request.receive $receive_watch 0
tg --url $server.url checkpoint unwatch indexer.request.receive $receive_watch
tg --url $server.url checkpoint wait indexer.request.wait $wait_watch 1 | ignore

# The first request must finish even while the later local wait remains blocked.
let output = job recv --tag $first --timeout 10sec
success $output
tg --url $server.url checkpoint unwatch indexer.request.wait $wait_watch
let output = job recv --tag $second --timeout 10sec
success $output

let metadata = tg --url $server.url object metadata $id | from json
assert ($metadata.subtree.count > 0)
