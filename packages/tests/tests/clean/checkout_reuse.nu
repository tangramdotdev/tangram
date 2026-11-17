use ../../util/test.nu *

# A sandbox can execute a reused checkout after its original ttl expires, including through a dependency cycle.

let file = 'tg.file({ "contents": tg.blob("#!/bin/sh\necho reused\n"), "executable": true })'
let cycle = 'tg.symlink({
	"graph": tg.graph({ "nodes": [
		{ "kind": "symlink", "artifact": 1 },
		{ "kind": "file", "contents": tg.blob("#!/bin/sh\necho reused\n"), "executable": true, "dependencies": { "link": { "node": 0 } } }
	] }),
	"index": 0,
	"kind": "symlink"
})'
let parent = ['tg.symlink({ "artifact": ' $cycle ' })'] | str join

for value in [$file $cycle $parent] {
	let local = server spawn --now '2026-01-01T00:00:00Z' --config {
		advanced: { checkpoints: true }
		indexer: { cleaning: {} }
		object: { ttl: 60, ttt: 0 }
		vfs: false
	}
	let artifact = tg put --no-tokens $value | referent node
	let unused = tg put --no-tokens 'tg.blob("unused")' | referent node
	tg index

	# Check out the artifact twice while its index writes are held.
	let write = checkpoint watch index.write
	tg checkout $artifact | ignore
	tg checkout $artifact | ignore
	checkpoint unwatch $write
	tg index

	# Reuse the checkout and hold the sandbox before it executes the script.
	advance_time $local 50sec
	let start = checkpoint watch runner.process.start
	let process = tg spawn --sandbox --executable $artifact | referent node
	checkpoint wait $start | ignore

	# Run the script after cleaning has passed the checkout's original expiry time.
	let delete = checkpoint watch cleaning.object.delete --params { object: $unused }
	advance_time $local 20sec
	checkpoint wait $delete | ignore
	checkpoint unwatch $delete
	checkpoint unwatch $start
	assert equal (tg wait $process | from json | get exit) 0
	assert equal (tg log $process | str trim) 'reused'

	server stop $local
}
