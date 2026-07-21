use ../../test.nu *

# Concurrent index requests share the indexer and all complete.

let server = spawn --config { indexer: { poll_interval: 0.01 } }
let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	'
}
let id = tg --url $server.url checkin $path

let outputs = 1..8 | par-each --threads 8 {
	tg --url $server.url index | complete
}
for output in $outputs {
	success $output
}

let metadata = tg --url $server.url object metadata $id | from json
assert ($metadata.subtree.count > 0)
