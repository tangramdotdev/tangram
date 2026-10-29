use ../../lib/test.nu *

# Checking in a directory with more entries than fit in a directory leaf produces a branched directory object.

let local = server spawn

# Create a directory with 1,025 entries, one more than fit in a directory leaf.
let entries = 0..<1_025 | each { |i| { name: $'($i | into string | fill --alignment right --character "0" --width 4).txt', contents: $'File ($i)' } } | transpose --header-row --as-record
let path = artifact $entries

let id = tg checkin --no-tokens $path | referent node
tg index

let object = tg object get --no-tokens --pretty $id
snapshot --name object $object

let metadata = tg object metadata --pretty $id
snapshot --name metadata $metadata
