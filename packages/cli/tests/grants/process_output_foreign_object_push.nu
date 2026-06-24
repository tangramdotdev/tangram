use ../../test.nu *

# Pushing a process that names a foreign object as its output must not leak the object on the remote: the remote's sync grant creation must not confer the subtree of an output the pusher cannot access.

let remote = spawn --cloud --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file on the remote.
let file = tg --url $alice_local.url put 'tg.file("topsecret")' | str trim
tg --url $alice_local.url index
tg --url $alice_local.url push $file
tg --url $remote.url index

# Eve does not have the subtree for Alice's private file on the remote.
let denied = tg --url $remote.url --token $eve.token get $file | complete
failure $denied "Eve should not have the subtree for Alice's private file before the exploit."

# Eve builds a process on her own server whose output names Alice's private file by id.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}
let source = 'export default function () { return tg.File.withId("FILE_ID"); }' | str replace "FILE_ID" $file
let eve_path = artifact { tangram.ts: $source }
let eve_process = tg --url $eve_local.url build --detach $eve_path | str trim
tg --url $eve_local.url wait $eve_process | complete

# Eve pushes her process and its output to the remote.
tg --url $eve_local.url push $eve_process --output | complete
tg --url $remote.url index

# Eve must not gain the subtree of Alice's private file on the remote by naming it as her process output.
let leaked = tg --url $remote.url --token $eve.token get $file | complete
failure $leaked "Eve must not gain the subtree of Alice's private file after pushing a process that names it as output."
