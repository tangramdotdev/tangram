use ../../test.nu *

# A private process is masked as not found until its owner grants the reader the subtree.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it to the remote.
let path = artifact {
	tangram.ts: '
		export default () => {
			return 5
		}
	'
}
let process = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $process
tg --url $alice_local.url index
tg --url $alice_local.url push $process
tg --url $remote.url index

# Eve cannot read Alice's private process; it is masked as not found rather than reported as unauthorized.
let denied = tg --url $remote.url --token $eve.token get $process | complete
failure $denied "Eve should not read Alice's private process."
assert (not ($denied.stderr | str contains "unauthorized")) "the masked read should not reveal that the process exists."

# After Alice grants Eve the process subtree, Eve can read it.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree $process | ignore
let output = tg --url $remote.url --token $eve.token get $process | complete
success $output "Eve should read the process after Alice grants the subtree."
