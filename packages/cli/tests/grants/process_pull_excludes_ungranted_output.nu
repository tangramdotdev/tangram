use ../../test.nu *

# The replication send-out path honors field grants: a principal granted every field but the output pulls the process, receiving the granted command object but not the ungranted output object.

let remote = spawn --name remote --config { authentication: true }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("hello"); }' }
let process = tg --url $remote.url --token $alice.token build --detach $path | str trim
tg --url $remote.url --token $alice.token wait $process | ignore

# Capture the command and output objects, which the owner can see, to probe what the pull carries.
let data = tg --url $remote.url --token $alice.token get $process | from json
let command_object = $data.command
let output_object = $data.output.value

# Alice grants Bob every process field except the output.
tg --url $remote.url --token $alice.token grant $bob.user.id process_node,process_node_command,process_node_log $process | ignore

# Bob's machine pulls the process from the remote.
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}
let pull = tg --url $bob_local.url --no-quiet pull $process --command --log | complete
success $pull "Bob's pull should carry the fields his grant covers."

# The granted command object is now resident on Bob's machine.
let command_read = tg --url $bob_local.url get $command_object --local | complete
success $command_read "the granted command object should be replicated."

# The ungranted output object is not, so the send-out path did not exfiltrate it.
let output_read = tg --url $bob_local.url get $output_object --local | complete
failure $output_read "the ungranted output object must not be replicated."
