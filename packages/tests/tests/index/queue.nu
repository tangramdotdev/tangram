use ../../lib/test.nu *

# An index batch split within its encoded items survives an indexer crash and is reassembled on restart.

let directory = mktemp -d
let config = {
	advanced: { checkpoints: true, multi_server: true },
	indexer: { id: 'idx_0000000000000000000000000000' },
	object: { index_queue: { fragment_size: 64 } },
}
let local = server spawn --name local --directory $directory --config $config
let watch = tg --url $local.url checkpoint watch index.batch | from json | get watch

let id = tg --url $local.url put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | referent node
tg --url $local.url checkpoint wait index.batch $watch 0 | ignore

let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid

let server = server start $local

tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert equal $metadata.subtree.count 5 "the object tree should be indexed after recovery"
