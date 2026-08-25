use ../../../test.nu *

# Cleaning a tag checkout removes its tag and ancestor entries, and a later checkout recreates them.

let server = server spawn
let artifact = artifact 'contents'
let specifier = 'foo/bar/baz'
let store = $server.directory | path join store
let tag_path = $store | path join $specifier

tg tag -p $specifier $artifact
let path = tg checkout $specifier | str trim
assert equal $path $tag_path "expected checkout to return the tag path"
assert ($tag_path | path exists --no-symlink) "expected checkout to create the tag entry"
assert (($store | path join foo/bar) | path exists) "expected checkout to create the ancestor entries"

tg clean
assert (not ($tag_path | path exists --no-symlink)) "expected cleaning to remove the tag entry"
assert (not (($store | path join foo) | path exists)) "expected cleaning to remove the ancestor entries"

let path = tg checkout $specifier | str trim
assert equal $path $tag_path "expected checkout to return the tag path again"
assert equal (open $tag_path) 'contents' "expected checkout to recreate the tag entry"
