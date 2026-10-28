use ../../util/test.nu *

# A put whose index items exceed NATS's default message limit is enqueued in several requests and indexed.
if (which nats-server | is-empty) {
	skip_test 'this test requires nats-server'
}
let port = port
let log = mktemp
let messenger = job spawn {
	^nats-server --addr 127.0.0.1 --port $port out+err> $log
}
wait_for_log { log: $log } 'Server is ready'
let local = server spawn --config {
	advanced: { multi_server: true },
	index: { map_size: 268_435_456 },
	indexer: { enqueue: { retry: { max_retries: 0 } } },
	messenger: { kind: 'nats', url: $'nats://127.0.0.1:($port)' },
	cache: { map_size: 268_435_456 },
}

# Put a tree of 8,192 files with unique contents, so that the index items for the put exceed one MiB.
let directories = 0..<8 | each { |d|
	let files = 0..<1_024 | each { |f| $'"($f)":' + 'tg.file("' + $'($d)_($f)' + '")' } | str join ','
	$'"($d)":' + 'tg.directory({' + $files + '})'
} | str join ','
let output = 'tg.directory({' + $directories + '})' | tg --url $local.url put --no-tokens | complete
success $output 'the large put must be enqueued within the NATS message limit'
let id = $output.stdout | referent node
tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert equal $metadata.subtree.count 16_393 'the large put must be indexed'

server stop $local
job kill $messenger
