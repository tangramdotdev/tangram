use ../../test.nu *

# Object metadata for an indexed file reports the node and the aggregated subtree.

let server = spawn

let id = tg put 'tg.file("hello")' | str trim
tg index

let metadata = tg object metadata $id | from json
assert equal ($metadata | columns) [node subtree] "the metadata should contain the node and the subtree"
assert ($metadata.node.size > 0) "the node size should not be zero"
assert equal $metadata.node.solved true "the file should be solved"
assert equal $metadata.subtree.count 2 "the subtree should count the file and its blob"
assert equal $metadata.subtree.depth 2 "the subtree depth should span the file and its blob"
