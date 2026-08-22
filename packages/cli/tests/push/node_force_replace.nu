use ../../test.nu *

# Force replaces conflicting destination nodes and their complete named subtrees during push.

let destination = spawn --cloud --name destination
let old_root = tg --url $destination.url group create tree | from json
let old_child = tg --url $destination.url group create tree/old | from json
let old_target = tg --url $destination.url put 'tg.file("old")' | str trim
tg --url $destination.url tag put tree/old/leaf $old_target
let old_leaf = tg --url $destination.url tag get tree/old/leaf | from json

let source = spawn --name source --config {
	remotes: { default: { url: $destination.url } }
}
let new_root = tg --url $source.url group create tree | from json
let new_child = tg --url $source.url group create tree/new | from json

let output = tg --url $source.url push --group-children tree | complete
failure $output "a push should not replace a conflicting node without force"
assert ($output.stderr | str contains "the specifier is already in use")

tg --url $source.url push --force --group-children tree

assert equal (
	tg --url $destination.url group get tree | from json | get id
) $new_root.id
assert equal (
	tg --url $destination.url group get tree/new | from json | get id
) $new_child.id
failure (
	tg --url $destination.url group get $old_root.id | complete
) "the replaced group should be deleted"
failure (
	tg --url $destination.url group get $old_child.id | complete
) "the replaced child should be deleted"
failure (
	tg --url $destination.url tag get $old_leaf.id | complete
) "the replaced descendant tag should be deleted"
