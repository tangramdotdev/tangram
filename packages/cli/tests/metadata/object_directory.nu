use ../../test.nu *

# Object metadata for an indexed directory aggregates the subtree over its entries.

let server = spawn

let id = tg put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim
tg index

let metadata = tg object metadata $id | from json
assert equal $metadata.subtree.count 5 "the subtree should count the directory, both files, and both blobs"
assert equal $metadata.subtree.depth 3 "the subtree depth should span the directory, a file, and a blob"
assert ($metadata.subtree.size > $metadata.node.size) "the subtree size should include the children"
