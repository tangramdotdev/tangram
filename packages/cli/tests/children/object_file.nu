use ../../test.nu *

# A file's children are its contents blob, and the blob itself has no children.

let server = spawn

let file_id = tg put 'tg.file("hello")' | str trim
let children = tg object children $file_id | from json
assert equal ($children | length) 1 "the file should have one child"
let blob_id = $children | get 0
assert ($blob_id | str starts-with "blb_") "the child should be a blob"

let blob_children = tg object children $blob_id | from json
assert equal $blob_children [] "the blob should have no children"
