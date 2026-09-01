use ../../test.nu *

# Force replaces conflicting local nodes and their complete named subtrees during pull.

let source = server spawn --cloud --name source
let new_root = tg --url $source.url group create tree | from json
let new_child = tg --url $source.url group create tree/new | from json

let destination = server spawn --name destination --config {
	remotes: { default: { url: $source.url } }
}
let old_root = tg --url $destination.url group create tree | from json
let old_child = tg --url $destination.url group create tree/old | from json
let old_target = tg --url $destination.url put 'tg.file("old")' | str trim
tg --url $destination.url tag put tree/old/leaf $old_target
let old_leaf = tg --url $destination.url tag get tree/old/leaf | from json
tg --url $destination.url group create keeper
tg --url $destination.url group members add keeper $old_root.id
tg --url $destination.url organization create company
tg --url $destination.url organization members add company $old_root.id
let runner = tg --url $destination.url runner create --owner tree | from json

let output = tg --url $destination.url pull --group-children tree | complete
failure $output "a pull should not replace a conflicting node without force"
assert ($output.stderr | str contains "the specifier is already in use")

tg --url $destination.url pull -f --group-children tree

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
let group_members = tg --url $destination.url group members list keeper | from json
assert not ($old_root.id in $group_members) "the replaced group should be removed from group memberships"
let organization_members = tg --url $destination.url organization members list company | from json
assert not ($old_root.id in $organization_members) "the replaced group should be removed from organization memberships"
let runner = (
	tg --url $destination.url runner list --all
	| from json
	| where id == $runner.data.id
	| first
)
assert (($runner | get --optional owner) == null) "the replaced group should be cleared as the runner owner"
