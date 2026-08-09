use ../../test.nu *

# Object subtree authorization must derive access from explicit grants when no materialized grant exists.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let value = 'tg.directory({ "hello.txt": tg.file("hello") })'
let directory = tg --token $alice.token put $value | str trim
tg --token $alice.token index
let child = tg --token $alice.token children $directory | from json | get 0

# These explicit grants prove object_subtree access to the directory.
tg --token $alice.token grant $eve.user.id object_node $directory | ignore
tg --token $alice.token grant $eve.user.id object_subtree $child | ignore
tg --token $alice.token index

let metadata = tg --token $eve.token metadata $directory | from json
assert equal $metadata.subtree.count 3 "the initial grants should confer subtree access"

# Cleaning removes the materialized directory grant but preserves the explicit grants.
tg --token $alice.token clean

# Restore the graph without changing its grants.
let restored = tg --token $alice.token put $value | str trim
assert equal $restored $directory "putting the same value should restore the same object"
tg --token $alice.token index

let metadata = tg --token $eve.token metadata $directory | from json
assert equal $metadata.subtree.count 3 "the explicit grants should still prove subtree access"
