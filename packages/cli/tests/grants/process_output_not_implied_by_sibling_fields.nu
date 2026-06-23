use ../../test.nu *

# Each process content field is conferred independently: a grant of every field but the output still masks the output value and leaves the output object unreadable.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

# Alice builds a process whose output is a file.
let path = artifact { tangram.ts: 'export default async function () { return tg.file("hello"); }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process | ignore

# Capture the output object, which the owner can see, to probe Bob's access.
let output_object = tg --token $alice.token get $process | from json | get output.value

# Alice grants Bob every process field except the output.
tg --token $alice.token grant $bob.user.id process_node,process_node_command,process_node_log,process_node_error $process

# Bob reads the process, but the output value is masked.
let bobget = tg --token $bob.token get $process | from json
assert ($bobget.output? == null) "a grant omitting the output field must mask the output value."

# Bob cannot read the output object the grant omits.
let denied = tg --token $bob.token get $output_object | complete
failure $denied "Bob must not read the output object his grant omits."
