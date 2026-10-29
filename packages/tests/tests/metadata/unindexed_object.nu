use ../../util/test.nu *

# Object metadata for an object that has not been indexed reports only the node.

let local = server spawn --config { roles: [api runner scheduler] }

let id = tg put --no-tokens 'tg.file("hello")' | referent node

let metadata = tg object metadata $id | from json
assert equal ($metadata | columns) [node] "the subtree should be absent before indexing"
assert ($metadata.node.size > 0) "the node size should not be zero"
