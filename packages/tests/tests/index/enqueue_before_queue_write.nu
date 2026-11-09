use ../../util/test.nu *

# An enqueue returns once its entries are indexed, without waiting for the cache's queue write.

let server = server spawn --config {
	advanced: { checkpoints: true, multi_server: true },
	indexer: { id: 'idx_0000000000000000000000000000' },
}

# Hold the queue write, and put an object, which enqueues its index entries.
let write = checkpoint watch indexer.queue.write
let object = tg put --no-tokens 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | referent node

# The put returned while the queue write is still held.
checkpoint wait $write | ignore

# The object is indexed once the queue write is released.
checkpoint unwatch $write
tg index
let metadata = tg object metadata $object | from json
assert equal $metadata.subtree.count 5 'the object tree should be indexed'
