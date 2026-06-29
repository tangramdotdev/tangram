use ../../test.nu *

# Publishing a non-leaf process is bounded by subtree access: a tagger with only node access to a parent whose child she cannot see must not, by tagging --public, expose that hidden child or its log.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

# Alice builds a parent process with a child whose log holds a secret.
let path = artifact { tangram.ts: r#'
	export function child() { console.log("childsecret"); return tg.file("c"); }
	export default async function () {
		await tg.build(child).named("c");
		return tg.file("parent");
	}
'# }
let parent = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $parent | complete
tg --token $alice.token index
let child = tg --token $alice.token process children $parent | from json | get 0 | get process

# Alice grants Bob only node access to the parent, not its subtree.
tg --token $alice.token grant $bob.user.id process_node $parent | ignore

# Bob reads the parent node but cannot reach the masked child.
let bob_parent = tg --token $bob.token get $parent | complete
success $bob_parent "Bob should read the parent node he was granted."
let bob_child = tg --token $bob.token get $child | complete
failure $bob_child "Bob should not reach the child the node grant does not confer."

# Bob publishes the parent with a public tag; the recording is bounded by his node-only access.
tg --token $bob.token tag put bobparent $parent --public
tg --token $bob.token index

# Eve resolves the public tag and reads the published parent node.
let eve_tag = tg --token $eve.token tag get bobparent | complete
success $eve_tag "Eve should resolve the public tag."
let eve_parent = tg --token $eve.token get $parent | complete
success $eve_parent "Eve should read the published parent node."

# But Eve must not reach the hidden child or its log: Bob could not see it, so he could not publish it.
let eve_child = tg --token $eve.token get $child | complete
failure $eve_child "a public process tag must not expose a child the tagger could not see."
let eve_child_log = tg --token $eve.token process log $child | complete
assert (not ($eve_child_log.stdout | str contains "childsecret")) "the hidden child's log must stay masked through the public tag."
