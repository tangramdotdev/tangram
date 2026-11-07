use ../../util/test.nu *

# When the sender reports a process missing, the receiver runs the full verification, which proves an organization member's access to the process.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Alice creates an organization that includes Bob.
let organization = tg --url $remote.url --token $alice.token organization create acme | from json
tg --url $remote.url --token $alice.token organization members add acme $bob.user.id

# Alice builds a parent process that calls a child, and pushes the whole tree privately to the remote.
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
let parent = tg --url $alice_local.url build --no-tokens --detach $path | referent node
tg --url $alice_local.url wait $parent
tg --url $alice_local.url index
tg --url $alice_local.url push $parent --process-children --process-command-objects --process-log-objects
tg --url $remote.url index

# Bob has only the parent process node, and Alice grants the organization the process subtree.
tg --url $alice_local.url get --no-tokens $parent | tg --url $bob_local.url put --no-tokens --id $parent | referent node
tg --url $bob_local.url index
tg --url $remote.url --token $alice.token grant $organization.id process_subtree $parent | ignore
tg --url $remote.url index --database

# Bob cannot send the child, so the remote proves it through his membership and the push succeeds.
let output = tg --url $bob_local.url --no-quiet push --lazy $parent --process-children --no-process-output-objects --no-process-error-objects | complete
success $output "Bob should rely on the child process through his organization."
assert ($output.stderr | str contains 'skipped 1 process') "The push should report the child process as skipped."
