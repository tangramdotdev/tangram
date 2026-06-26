use ../../test.nu *

# A field-scoped process grant must not let a reader pull a different field through sync. Alice grants Eve the process subtree and its output, but not the command; pulling the process with its command must not ship the command object.

let remote = spawn --cloud --name remote --config { authentication: { providers: { insecure: true } } }

let alice = tg --url $remote.url login --verbose alice | from json
let eve = tg --url $remote.url login --verbose eve | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it with its command and output.
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }
let process = tg --url $alice_local.url build --detach $path | str trim
tg --url $alice_local.url wait $process
tg --url $alice_local.url index
tg --url $alice_local.url push $process --command --output
tg --url $remote.url index
let data = tg --url $alice_local.url get $process | from json

# Alice grants Eve the process subtree and its output, but not the command.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree $process | ignore
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree_output $process | ignore

# Sanity: Eve can get the output she was granted but not the command.
let output = tg --url $remote.url --token $eve.token get $data.output.value | complete
success $output "Eve should get the granted output object."
let command = tg --url $remote.url --token $eve.token get $data.command | complete
failure $command "Eve should not get the command she was not granted."

# Eve has her own server that talks to the remote as Eve.
let eve_local = spawn --name eve-local --config {
	remotes: { default: { url: $remote.url, token: $eve.token } },
}

# Eve must not be able to pull the command field she was not granted.
let pulled = tg --url $eve_local.url pull $process --command | complete
failure $pulled "Eve must not pull the command field of a process whose node and output she has but not its command."
