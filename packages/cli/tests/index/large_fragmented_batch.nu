use ../lib/test.nu *

# An index batch larger than NATS's default message limit must be split into byte fragments.
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
	indexer: { batch: { retry: { max_retries: 0 } } },
	messenger: { kind: 'nats', url: $'nats://127.0.0.1:($port)' },
	cache: { map_size: 268_435_456 },
}

# Put a tree of 8,192 files with unique contents, so that the index batch for the put exceeds one MiB.
let directories = 0..<8 | each { |d|
	let files = 0..<1_024 | each { |f| $'"($f)":' + 'tg.file("' + $'($d)_($f)' + '")' } | str join ','
	$'"($d)":' + 'tg.directory({' + $files + '})'
} | str join ','
let output = 'tg.directory({' + $directories + '})' | tg --url $local.url put --no-tokens | complete
success $output 'the large index batch must fit within the NATS message limit'
let id = $output.stdout | referent node
tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert equal $metadata.subtree.count 16_393 'the large index batch must be indexed'

server stop $local
job kill $messenger
