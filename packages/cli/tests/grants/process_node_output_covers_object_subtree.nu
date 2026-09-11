use ../../test.nu *

# Node output permissions cover this process's object subtrees, not child processes' outputs.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a parent and child with distinct output object subtrees.
let path = artifact {
	tangram.ts: '
		export default async function () {
			await tg.build(child);
			return tg.directory({ nested: tg.directory({ file: tg.file("parent output") }) });
		}
		export function child() {
			return tg.directory({ nested: tg.directory({ file: tg.file("child output") }) });
		}
	',
}
let parent = tg --token $alice.token build --detach $path | str trim
let output = (tg --token $alice.token wait $parent | from json).output.value | split row '?' | first
tg --token $alice.token index
let data = tg --token $alice.token get $parent | from json
let child = $data.children.0.process | split row '?' | first
let child_output = (tg --token $alice.token wait $child | from json).output.value | split row '?' | first

# A node output grant allows checking out the parent's whole output subtree.
tg --token $alice.token grant $eve.user.id process_node_output $parent
let directory = mktemp --directory
let parent_path = $directory | path join parent
success (tg --token $eve.token checkout $output --path $parent_path | complete) "a node output grant should authorize the output subtree."
assert equal (open --raw ($parent_path | path join nested file)) "parent output"

# The process node, command, and child output remain unreadable.
failure (tg --token $eve.token get $parent | complete) "a node output grant must not authorize the process node."
failure (tg --token $eve.token get $data.command | complete) "a node output grant must not authorize the command."
failure (tg --token $eve.token get $child_output | complete) "a node output grant must not authorize a child process's output."

# A subtree output grant also allows checking out the child's whole output subtree.
tg --token $alice.token grant $eve.user.id process_subtree_output $parent
let child_path = $directory | path join child
success (tg --token $eve.token checkout $child_output --path $child_path | complete) "a subtree output grant should authorize a child process's output subtree."
assert equal (open --raw ($child_path | path join nested file)) "child output"
