use ../lib/test.nu *

# Indexing uses the index queue when the server is not in single process mode.

let local = server spawn --config {
	advanced: { single_process: false },
	database: {
		kind: 'sqlite',
		path: 'database.sqlite3',
	},
	indexer: {
		id: 'idx_0000000000000000000000000000',
	},
	object: {
		archive_queue: { sequence_reservation_size: 2 },
		index_queue: {
			fragment_size: 64,
			sequence_reservation_size: 2,
		},
		queue_checkpoint_interval: 0.01,
	},
}
let group = tg --url $local.url group create project | from json
tg --url $local.url index
let indexed = tg --url $local.url group get project | from json
assert equal $indexed.id $group.id
let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	'
}
let id = tg --url $local.url checkin $path

tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert ($metadata.subtree.count > 0)
