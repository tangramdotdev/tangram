use ../../test.nu *

# A process grant that covers every resident field but one still denies reliance, since each field is conferred independently and the missing field's objects stay invisible.

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
		export default async () => {
			return await tg.build(child)
		}

		export const child = () => {
			return tg.file("hello")
		}
	'
}
let parent = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $parent
tg --url $alice_local.url index
tg --url $alice_local.url push $parent --recursive --command --log
tg --url $remote.url index

# Bob has only the parent process node and is granted the command and log fields but not the resident output field.
tg --url $alice_local.url get $parent | tg --url $bob_local.url put --id $parent
tg --url $bob_local.url index
tg --url $remote.url --token $alice.token grant $bob.user.id process_subtree,process_subtree_command,process_subtree_log $parent | ignore

# The missing output field leaves the resident output objects invisible, so Bob cannot rely on them and the recursive lazy push fails.
let output = tg --url $bob_local.url --no-quiet push --lazy $parent --recursive --command --log | complete
failure $output "Bob should not rely on the output field his grant omits."
