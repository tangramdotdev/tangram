use ../lib/test.nu *

# Object metadata for an object that has not been indexed reports only the node.

let server = server spawn --config { roles: [api runner scheduler] }

let id = tg put 'tg.file("hello")' | str trim

wait_until {
	(tg object metadata $id | complete).exit_code == 0
} --timeout 10sec "the node metadata should become available without indexing"
let metadata = tg object metadata $id | from json
assert equal ($metadata | columns) [node] "the subtree should be absent before indexing"
assert ($metadata.node.size > 0) "the node size should not be zero"
