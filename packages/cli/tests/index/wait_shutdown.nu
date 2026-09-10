use ../../test.nu *

# A departing indexer leaves shared work for a fresh wait, even if the replacement already responded.
if (which nats-server | is-empty) {
	skip_test 'this test requires nats-server'
}
let port = port
let log = mktemp
let messenger = job spawn {
	^nats-server --addr 127.0.0.1 --port $port out+err> $log
}
wait_until { open --raw $log | str contains 'Server is ready' } 'NATS must start'
let directory = mktemp -d
let config = {
	advanced: { checkpoints: true, single_directory: false, single_process: false },
	database: { kind: 'sqlite', path: ($directory | path join database.sqlite3) },
	index: { kind: 'lmdb', map_size: 268_435_456, path: ($directory | path join index) },
	indexer: { cache: { poll_interval: 0.01 }, cleaning: false },
	messenger: { kind: 'nats', url: $'nats://127.0.0.1:($port)' },
	roles: [api indexer],
	store: {
		kind: 'lmdb',
		map_size: 268_435_456,
		path: ($directory | path join store),
		posix_sem_prefix: $'/tg-((random chars) | str lowercase | str substring 0..7)',
	},
}
let a = server spawn --name a --config ($config | merge deep {
	indexer: {
		log_compaction: { partitions: { start: 0, end: 0 } },
		updates: {
			grants: { partitions: { start: 0, end: 0 } },
			nodes: { partitions: { start: 0, end: 0 } },
			storage: { partitions: { start: 0, end: 0 } },
		},
	},
})
let batch_watch = tg --url $a.url checkpoint watch index.batch | from json | get watch
let a_wait_watch = tg --url $a.url checkpoint watch indexer.request.receive | from json | get watch

# Only A exists when the batch is submitted, so its private queue owns the work.
let object = tg --url $a.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg --url $a.url checkpoint wait index.batch $batch_watch 0 | ignore
let b = server spawn --name b --config $config
let update_watch = tg --url $b.url checkpoint watch indexer.update.node.batch | from json | get watch
tg --url $b.url checkpoint wait indexer.update.node.batch $update_watch 0 | ignore
let api = server spawn --name api --config ($config | upsert roles [api])
let params = { indexer: $b.config.indexer.id } | to json --raw
let complete_watch = tg --url $api.url checkpoint watch indexer.wait.complete --params $params | from json | get watch
let url = $api.url
let request = job spawn {
	let id = job id
	let output = tg --url $url index | complete
	$output | job send --tag $id 0
}

# B responds while A still has its input queued and its wait request is held.
tg --url $a.url checkpoint wait indexer.request.receive $a_wait_watch 0 | ignore
tg --url $api.url checkpoint wait indexer.wait.complete $complete_watch 0 | ignore
tg --url $api.url checkpoint unwatch indexer.wait.complete $complete_watch
let b_wait_watch = tg --url $b.url checkpoint watch indexer.request.receive | from json | get watch

# A must shut down after applying its private batch, although B is holding the shared updates.
tg --url $a.url checkpoint unwatch index.batch $batch_watch
let pid = open ($a.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } 'A must shut down without draining shared work'

# The caller must send B another wait instead of treating A's deletion as completion.
tg --url $b.url checkpoint wait indexer.request.receive $b_wait_watch 0 | ignore
tg --url $b.url checkpoint continue indexer.request.receive $b_wait_watch 0
let pending = try {
	job recv --tag $request --timeout 200ms | ignore
	false
} catch {
	true
}
assert $pending 'the replacement wait must include the shared updates'
tg --url $b.url checkpoint unwatch indexer.update.node.batch $update_watch
let output = job recv --tag $request --timeout 10sec
success $output 'the replacement wait must finish after the shared updates'
let metadata = tg --url $api.url object metadata $object | from json
assert equal $metadata.subtree.count 5 'the departed indexer must preserve all of its input'

# Losing the last indexer while a wait is outstanding must return an error.
let request = job spawn {
	let id = job id
	let output = tg --url $url index | complete
	$output | job send --tag $id 0
}
tg --url $b.url checkpoint wait indexer.request.receive $b_wait_watch 1 | ignore
let pid = open ($b.directory | path join lock) | into int
kill --signal 2 $pid
wait_until { ps | where pid == $pid | is-empty } 'B must shut down with an outstanding wait'
let output = job recv --tag $request --timeout 10sec
failure $output 'the last indexer disappearing must not complete indexing'
snapshot --normalize $output.stderr '
	error an error occurred
	-> no indexers are available

'

server stop $api
job kill $messenger
