use ../lib/test.nu *

# Force replaces conflicting destination nodes and their complete named subtrees during push.

let remote_destination = server spawn --cloud --name remote-destination
let old_root = tg --url $remote_destination.url group create tree | from json
let old_child = tg --url $remote_destination.url group create tree/old | from json
let old_target = tg --url $remote_destination.url put --no-tokens 'tg.file("old")' | referent node
tg --url $remote_destination.url tag put tree/old/leaf $old_target
let old_leaf = tg --url $remote_destination.url tag get tree/old/leaf | from json

let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote_destination.url } }
}
let new_root = tg --url $local_source.url group create tree | from json
let new_child = tg --url $local_source.url group create tree/new | from json

let output = tg --url $local_source.url push --group-children tree | complete
failure $output "a push should not replace a conflicting node without force"
assert ($output.stderr | str contains "the specifier is already in use")

tg --url $local_source.url push --force --group-children tree

assert equal (
	tg --url $remote_destination.url group get tree | from json | get id
) $new_root.id
assert equal (
	tg --url $remote_destination.url group get tree/new | from json | get id
) $new_child.id
failure (
	tg --url $remote_destination.url group get $old_root.id | complete
) "the replaced group should be deleted"
failure (
	tg --url $remote_destination.url group get $old_child.id | complete
) "the replaced child should be deleted"
failure (
	tg --url $remote_destination.url tag get $old_leaf.id | complete
) "the replaced descendant tag should be deleted"
