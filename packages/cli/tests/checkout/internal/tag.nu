use ../../../test.nu *

# An internal checkout creates a tag checkout entry, while tag mutations only invalidate it.

let server = spawn
let tag_path = $server.directory | path join store dep

let first = artifact 'first'
tg tag dep $first
assert (not ($tag_path | path exists --no-symlink)) "expected putting a new tag not to create its store entry"

let path = tg checkout dep | str trim
assert equal $path $tag_path "expected checkout to return the tag path"
assert (($tag_path | path exists --no-symlink)) "expected checkout to create the tag checkout entry"
assert equal (open $tag_path) 'first' "expected the tag path to contain the first artifact"

let second = artifact 'second'
tg tag dep $second
assert (not ($tag_path | path exists --no-symlink)) "expected replacing the tag to invalidate its store entry"

let path = tg checkout dep | str trim
assert equal $path $tag_path "expected checkout to return the tag path"
assert (($tag_path | path exists --no-symlink)) "expected checkout to recreate the tag checkout entry"
assert equal (open $tag_path) 'second' "expected the tag path to contain the second artifact"

tg tag delete dep | ignore
assert (not ($tag_path | path exists --no-symlink)) "expected deleting the tag to invalidate its store entry"
