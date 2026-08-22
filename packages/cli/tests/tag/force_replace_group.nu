use ../../test.nu *

# Force replaces a conflicting group and its complete named subtree with a tag.

let server = spawn
let root = tg group create tree | from json
let child = tg group create tree/child | from json
let old_target = tg put 'tg.file("old")' | str trim
tg tag put tree/child/leaf $old_target
let leaf = tg tag get tree/child/leaf | from json
let target = tg put 'tg.file("new")' | str trim

let output = tg tag put tree $target | complete
failure $output "a tag should not replace a group without force"
assert ($output.stderr | str contains "specifier is already in use")

tg tag put -f tree $target

let tag = tg tag get tree | from json
assert equal $tag.target.id $target
failure (tg group get $root.id | complete) "the replaced group should be deleted"
failure (tg group get $child.id | complete) "the replaced child should be deleted"
failure (tg tag get $leaf.id | complete) "the replaced descendant tag should be deleted"
