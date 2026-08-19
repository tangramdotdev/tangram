use ../../test.nu *

# Indexing uses the outbox when the server is not in single process mode.

let server = spawn --config {
	advanced: { single_process: false },
	database: {
		kind: 'sqlite',
		path: 'database',
	},
	indexer: {
		partition_end: 4,
	},
	object: {
		outbox: { partition_total: 4 },
	},
}
let group = tg --url $server.url group create project | from json
tg --url $server.url index
let indexed = tg --url $server.url group get project | from json
assert equal $indexed.id $group.id
let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	'
}
let id = tg --url $server.url checkin $path

tg --url $server.url index
let metadata = tg --url $server.url object metadata $id | from json
assert ($metadata.subtree.count > 0)
