use ../../test.nu *

# A grant on the process error field confers only the error object, leaving the process node and other fields masked.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process that fails, so its error is stored as an object.
let path = artifact { tangram.ts: 'export default function () { throw new Error("secreterror") }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process | complete
tg --token $alice.token index
let data = tg --token $alice.token get $process | from json

# The failed process stores its error as an object rather than inline data.
assert (($data.error | to json) | str starts-with '"err_') ("the failed process should store its error as an object: " + ($data | to json))
let error = $data.error

# Alice grants Eve only the error field of the process subtree.
tg --token $alice.token grant $eve.user.id process_subtree_error $process | ignore

# Eve can read the error object the grant covers.
let error_read = tg --token $eve.token get $error | complete
success $error_read "Eve should read the granted error object."

# The process node is not covered by the error grant, so it stays masked.
let node = tg --token $eve.token get $process | complete
failure $node "the error grant should not confer the process node."

# The command object is a different field, so it stays masked.
let command = tg --token $eve.token get $data.command | complete
failure $command "the error grant should not confer the command object."
