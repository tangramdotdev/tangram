use ../../test.nu *

# A single index item larger than NATS's default message limit must be split into byte fragments.
if (which nats-server | is-empty) {
	skip_test 'this test requires nats-server'
}
let port = port
let log = mktemp
let messenger = job spawn {
	^nats-server --addr 127.0.0.1 --port $port out+err> $log
}
wait_until { open --raw $log | str contains 'Server is ready' } 'NATS must start'
let server = server spawn --config {
	advanced: { single_process: false },
	index: { map_size: 268_435_456 },
	indexer: { batch: { retry: { max_retries: 0 } } },
	messenger: { kind: 'nats', url: $'nats://127.0.0.1:($port)' },
	store: { map_size: 268_435_456 },
}

# Unique children make the directory's single index item exceed one MiB.
let entries = 0..<30_000 | each { |i|
	let hash = $i | into string | fill --alignment right --character '0' --width 51
	{ name: ($i | into string), id: $'fil_01($hash)0' }
} | transpose --header-row --as-record
let output = { entries: $entries } | to json --raw | tg --url $server.url object put --bytes --kind directory | complete
success $output 'the large index item must fit within the NATS message limit'
let id = $output.stdout | str trim
tg --url $server.url index
let metadata = tg --url $server.url object metadata $id | from json
assert ($metadata.node.size > 1_048_576) 'the large directory must be indexed'

server stop $server
job kill $messenger
