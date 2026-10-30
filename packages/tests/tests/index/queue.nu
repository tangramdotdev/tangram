use ../../util/test.nu *

# Queued index entries survive an indexer crash and are processed on restart.

let directory = mktemp -d
let config = {
	advanced: { checkpoints: true, multi_server: true },
	indexer: {
		enqueue: { request_size: 64 },
		id: 'idx_0000000000000000000000000000',
	},
}
let local = server spawn --name local --directory $directory --config $config
let watch = tg --url $local.url checkpoint watch index.write | from json | get watch

let id = tg --url $local.url put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | referent node
tg --url $local.url checkpoint wait index.write $watch 0 | ignore

let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

let server = server start $local

tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert equal $metadata.subtree.count 5 "the object tree should be indexed after recovery"
