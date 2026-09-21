use ../../test.nu *

# A grant on the process error field confers only the error object, leaving the process node and other fields masked.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a private process that fails, so its error is stored as an object.
let path = artifact { tangram.ts: 'export default function () { throw new Error("secreterror") }' }
let input = tg --token $alice.token checkin (artifact "private input") | str trim
let process = tg --token $alice.token build --detach $path --arg-value $input | str trim
let result = tg --token $alice.token wait $process | from json
assert ($result.exit != 0) "the process must fail"
assert ($result.error | str starts-with 'err_') "the error must be stored as an object"
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

# The unrelated command input is not part of the error's source locations.
let command = tg --token $eve.token get $input | complete
failure $command "the error grant should not confer an unrelated command input."

# A node grant allows waiting on the token-free stored process data.
tg --token $alice.token grant $eve.user.id process_node $process | ignore
let result = tg --token $eve.token wait $process | from json
assert equal $result.error $error
