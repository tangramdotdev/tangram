use ../lib/test.nu *

# A process object receives the same later capture upgrades as a tag target.
let root = random chars
let local = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root }, users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let file = tg --token $root put --no-tokens 'tg.file("later grant")' | referent node
let command = tg --token $root put --no-tokens 'tg.command({"executable":"/bin/true","host":"x86_64-linux"})' | referent node
let process = 'pcs_00081061050r3gg28a1c60t3gf20'
let data = {
	children: []
	command: $command
	created_at: 0
	exit: 0
	finished_at: 1
	host: 'x86_64-linux'
	output: { kind: object, value: $file }
	status: finished
}
let params = { resource: $file, process: $process } | to json --raw
let advanced = tg --token $root checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --token $alice.token process put $process ($data | to json)
tg --token $root grant $bob.user.id process_node_output_objects $process | ignore
timeout 30s tg --token $root index
timeout 30s tg --token $root checkpoint wait permission_capture.advanced $advanced 0 | ignore
tg --token $root checkpoint unwatch permission_capture.advanced $advanced
failure (tg --token $bob.token get --bytes $file | complete) "naming a process output must not confer access before capture"

let written = tg --token $root checkpoint watch permission_capture.written --params $params | from json | get watch
tg --token $root grant $alice.user.id object_subtree $file | ignore
tg --token $root index
timeout 30s tg --token $root checkpoint wait permission_capture.written $written 0 | ignore
tg --token $root checkpoint unwatch permission_capture.written $written
tg --token $root revoke $alice.user.id object_subtree $file | ignore
tg --token $root index
success (tg --token $bob.token get --bytes $file | complete) "the process must retain the captured output permissions"
