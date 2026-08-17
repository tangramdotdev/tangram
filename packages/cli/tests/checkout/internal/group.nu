use ../../../test.nu *

# A group checkout materializes its ancestor directories and participates in checkout cleaning.

let server = spawn
let root = tg group create foo | from json
let group = tg group create foo/bar | from json
let store = $server.directory | path join store
let group_path = $store | path join foo/bar

let path = tg checkout $group.id | str trim
assert equal $path $group_path "expected checkout to return the group path"
assert ($group_path | path exists) "expected checkout to create the group directory"

tg clean
assert (not ($group_path | path exists)) "expected cleaning to remove the group directory"
assert (not (($store | path join foo) | path exists)) "expected cleaning to remove the ancestor directory"

let path = tg checkout $root.id | str trim
assert equal $path ($store | path join foo) "expected checkout to recreate the root group directory"

tg group delete foo/bar
tg group delete foo
assert (not (($store | path join foo) | path exists)) "expected deleting the group to remove its checkout directory"
