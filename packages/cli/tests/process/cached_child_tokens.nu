use ../lib/test.nu *

# Indexed cached children retain the remote authorization tokens needed to read their outputs.

let remote_root = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $remote_root }, users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let remote_bob = tg --url $remote.url login --verbose --name bob | from json
let path = artifact {
	tangram.ts: '
		export function child() { return tg.file("private cached output"); }
		export default async function () {
			await tg.build(child).cached(true);
			return "done";
		}
	',
}
let child = tg --url $remote.url --token $alice.token build --no-tokens --detach $'($path)#child' | referent node
tg --url $remote.url --token $alice.token wait --source=index $child | ignore
tg --url $remote.url --token $remote_root index
let child_data = tg --url $remote.url --token $alice.token process get --source=index --local $child | from json
let file = $child_data.output.value | referent node
failure (tg --url $remote.url --token $remote_bob.token wait --source=index $child | complete) "an unrelated user must not read the cached process"

let local_root = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $local_root }, users: { providers: { insecure: true } } },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob = tg --url $local.url login --verbose --name bob | from json
let carol = tg --url $local.url login --verbose --name carol | from json
tg --url $local.url --token $bob.token remote put default $remote.url
let parent = tg --url $local.url --token $local_root build --no-tokens --detach $path | referent node
tg --url $local.url --token $local_root wait --source=index $parent | ignore
tg --url $local.url --token $local_root index
tg --url $local.url --token $local_root grant $bob.user.id process_parent $parent | ignore
tg --url $local.url --token $local_root grant $carol.user.id process_node $parent | ignore
failure (tg --url $local.url --token $local_root object get --bytes --local $file | complete) "the cached output should remain on the remote"
failure (tg --url $local.url --token $bob.token wait --source=index --remote $child | complete) "the local parent relationship alone must not authorize access on the remote"

# A node reader can list the child without receiving its authorization tokens.
let children = tg --url $local.url --token $carol.token process children --source=index --local $parent | from json
assert equal ($children | length) 1
assert ($children.0.process | referent tokens remote | is-empty) "a node reader must not receive the child's parent authorization token"

# A parent reader can use the persisted token even without credentials for the remote owner.
let children = tg --url $local.url --token $bob.token process children --source=index --local $parent | from json
assert equal ($children | length) 1
assert equal ($children.0.process | referent node) $child
assert equal $children.0.cached true
let tokens = $children.0.process | referent tokens remote
assert ($tokens | is-not-empty) "the indexed child should retain its remote authorization token"
let body = $tokens.0 | token body
assert equal $body.resource $child
assert equal $body.permissions [process_parent]
let location = $'http://localhost/($children.0.process)' | url parse | get params | where key == location | get value | first
assert equal $location remote "the indexed child should retain its remote location"
let outcome = tg --url $local.url --token $bob.token wait --source=index $children.0.process | from json
let output = tg --url $local.url --token $bob.token cat $outcome.output.value | complete
success $output "the stored child token should authorize its remote output"
assert equal $output.stdout 'private cached output'
