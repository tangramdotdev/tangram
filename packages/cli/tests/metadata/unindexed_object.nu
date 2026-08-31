use ../../test.nu *

# Object metadata for an object that has not been indexed reports only the node.

let server = server spawn --config { roles: [http runner scheduler] }

let id = tg put 'tg.file("hello")' | str trim

let metadata = tg object metadata $id | from json
assert equal ($metadata | columns) [node] "the subtree should be absent before indexing"
assert ($metadata.node.size > 0) "the node size should not be zero"
