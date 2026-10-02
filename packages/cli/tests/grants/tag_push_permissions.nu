use ../lib/test.nu *

# Pushing a tag captures only the permissions available at the destination.

let root_token = random chars
let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	remotes: { default: { url: $remote.url, token: $alice.token } }
}
let reader = tg --url $local.url --token $root_token login --verbose --name reader | from json

# A local public tag confers the process node and its output on a reader who did not build it.
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }
let process = tg --url $local.url --token $root_token build --no-tokens --detach $path | referent node
let finished = tg --url $local.url --token $root_token wait $process | from json
let output = $finished.output.value | split row '?' | first
tg --url $local.url --token $root_token index
tg --url $local.url --token $root_token tag put process $process --public
tg --url $local.url --token $root_token index
let source = tg --url $local.url --token $reader.token get $process | from json
assert equal ($source.output.value | split row '?' | first) $output
success (tg --url $local.url --token $reader.token get $output | complete) "the source tag should confer access to its output object."

# Transfer only the process node; the tag must not confer output object permissions.
failure (tg --url $remote.url --token $alice.token get $output | complete) "the output must not already be available at the destination."
tg --url $local.url --token $root_token push --no-process-output-objects process
tg --url $remote.url --token $alice.token index
let tag = tg --url $remote.url --token $alice.token tag get process | from json
assert equal $tag.target.id $process
failure (tg --url $remote.url --token $bob.token get $process | complete) "Bob should have no direct access to the imported process."
tg --url $remote.url --token $alice.token grant $bob.user.id tag_read $tag.id | ignore
tg --url $remote.url --token $alice.token index
let destination = tg --url $remote.url --token $bob.token get $process | from json
assert equal $destination.status finished
let target = tg --url $remote.url --token $bob.token children --verbose $tag.id | from json | get data.0
let permissions = $target.options.tokens.local | each {|token|
	$token | split row '.' | get 1 | decode base64 | decode utf-8 | from json | get permissions
} | flatten
assert ($permissions | any {|permission| $permission == 'process_node' or $permission == 'process_subtree' }) "the imported tag must confer access to the process node."
assert (not ($permissions | any {|permission| $permission == 'process_node_output_objects' or $permission == 'process_subtree_output_objects' })) "the imported tag must not confer access to the untransferred output objects."
failure (tg --url $remote.url --token $bob.token get $output | complete) "the destination must not expose the untransferred output."
