use ../../test.nu *

# A process subtree grant covering each transferred field lets the grantee rely on a private child process subtree the remote already holds.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Alice builds a parent process that calls a child, and pushes the whole tree privately to the remote. A push includes outputs by default, so the command, log, and output subtrees all become resident.
let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(child)
		}

		export function child() {
			return tg.file("hello")
		}
	'
}
let parent = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $parent
tg --url $alice_local.url index
tg --url $alice_local.url push $parent --recursive --command --log
tg --url $remote.url index

# Bob has only the parent process node and relies on the granted subtree for the rest.
tg --url $alice_local.url get $parent | tg --url $bob_local.url put --id $parent
tg --url $bob_local.url index
tg --url $remote.url --token $alice.token grant $bob.user.id process_subtree,process_subtree_command,process_subtree_log,process_subtree_output $parent | ignore

# The grant covers every resident field, so Bob relies on the private child subtree and the recursive lazy push succeeds.
let output = tg --url $bob_local.url --no-quiet push --lazy $parent --recursive --command --log | complete
success $output "Bob should rely on the granted process subtree."
