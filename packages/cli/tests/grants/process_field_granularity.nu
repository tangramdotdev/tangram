use ../../test.nu *

# A grant on one process field confers only that field, leaving the process node and other fields masked.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it with its command and output.
let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("hello")
		}
	'
}
let parent = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $parent
tg --url $alice_local.url index
tg --url $alice_local.url push $parent --command --log
tg --url $remote.url index
let data = tg --url $alice_local.url get $parent | from json

# Alice grants Eve only the output field of the process subtree.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree_output $parent | ignore

# Eve can read the output object the grant covers.
let output = tg --url $remote.url --token $eve.token get $data.output.value | complete
success $output "Eve should read the granted output object."

# The process node is not covered by the output grant, so it stays masked.
let node = tg --url $remote.url --token $eve.token get $parent | complete
failure $node "the output grant should not confer the process node."

# The command object is a different field, so it stays masked.
let command = tg --url $remote.url --token $eve.token get $data.command | complete
failure $command "the output grant should not confer the command object."
