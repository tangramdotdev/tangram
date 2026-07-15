use ../../test.nu *

# A grant on the process command field confers only the command object, leaving the process node and other fields masked.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process.
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }
let parent = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $parent
tg --token $alice.token index
let data = tg --token $alice.token get $parent | from json

# Alice grants Eve only the command field of the process subtree.
tg --token $alice.token grant $eve.user.id process_subtree_command $parent | ignore

# Eve can read the command object the grant covers.
let command = tg --token $eve.token get $data.command | complete
success $command "Eve should read the granted command object."

# The process node is not covered by the command grant, so it stays masked.
let node = tg --token $eve.token get $parent | complete
failure $node "the command grant should not confer the process node."

# The output is a different field, so it stays masked.
let output = tg --token $eve.token get $data.output.value | complete
failure $output "the command grant should not confer the output object."
