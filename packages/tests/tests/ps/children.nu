use ../../util/test.nu *

# A child process is created by its parent process, so tg ps lists only the processes the user started, and listing by owner includes the children.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

let path = artifact {
	tangram.ts: '
		export const child = () => "child";
		export default async () => {
			const command = await tg.command(child);
			await command.store();
			const process = await tg.Process.spawnSandboxed({
				command: tg.Object.toReferent(command),
				sandbox: {},
				stderr: "log", stdin: "null", stdout: "log",
			});
			await process.wait();
			return process.id;
		};
	',
}
let output = tg --token $alice.token build --detach --verbose $path | from json
let parent = $output.process | split row '?' | first
tg --token $alice.token process wait $parent | ignore
let child = tg --token $alice.token process output $parent | from json
tg index

# The child's creator is its parent, and its owner is the parent's owner.
let data = tg --token $alice.token process get $child | from json
assert equal $data.creator $parent "the child creator should be the parent process"
assert equal $data.owner $alice.user.id "the child owner should be the parent's owner"

# Listing by creator includes the parent but not the child.
let created = tg --token $alice.token ps --any-status | from json
assert (($created | where node == $parent | is-empty) == false) "Alice should see the process she started"
assert ($created | where node == $child | is-empty) "Alice must not see the child in the processes she created"

# Listing by owner includes both.
let owned = tg --token $alice.token ps --any-status --owner $alice.user.id | from json
assert (($owned | where node == $parent | is-empty) == false) "Alice should see her process when listing by owner"
assert (($owned | where node == $child | is-empty) == false) "Alice should see the child when listing by owner"
