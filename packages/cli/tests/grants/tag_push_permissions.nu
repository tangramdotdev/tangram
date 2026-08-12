use ../../test.nu *

# Pushing a tag records only the permissions available at the destination, rather than copying the source tag's permissions.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# The local root records every process permission on the source tag.
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }
let process = tg --url $local.url build --detach $path | str trim
tg --url $local.url wait $process
tg --url $local.url index
tg --url $local.url tag put process $process
let source = tg --url $local.url tag get process | from json
assert ($source.permissions | any {|permission| $permission == "process_subtree_output" })

# The push transfers only the process node, so the destination tag must not confer access to its output.
tg --url $local.url push --no-process-outputs process
tg --url $remote.url index
let destination = tg --url $remote.url --token $alice.token tag get process | from json
assert ($destination.permissions | any {|permission| $permission == "process_node" or $permission == "process_subtree" })
assert (not ($destination.permissions | any {|permission| $permission == "process_node_output" or $permission == "process_subtree_output" }))
