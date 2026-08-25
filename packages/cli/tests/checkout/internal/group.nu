use ../../../test.nu *

# A group checkout materializes its visible subtree and participates in checkout cleaning.

let server = server spawn
let root = tg group create foo | from json
let group = tg group create foo/bar | from json
let store = $server.directory | path join store
let group_path = $store | path join foo/bar

let artifact = artifact 'test'
tg tag foo/bar/version $artifact
let tag_path = $group_path | path join version

let path = tg checkout foo | str trim
assert equal $path ($store | path join foo) "expected checkout to return the root group path"
assert ($group_path | path exists) "expected checkout to create the group directory"
assert equal (open $tag_path) test "expected checkout to materialize the descendant tag"

tg clean
assert (not ($group_path | path exists)) "expected cleaning to remove the group directory"
assert (not (($store | path join foo) | path exists)) "expected cleaning to remove the ancestor directory"

let path = tg checkout foo | str trim
assert equal $path ($store | path join foo) "expected checkout to recreate the root group directory"

tg tag delete foo/bar/version | ignore
tg group delete foo/bar
tg group delete foo
assert (not (($store | path join foo) | path exists)) "expected deleting the group to remove its checkout directory"
