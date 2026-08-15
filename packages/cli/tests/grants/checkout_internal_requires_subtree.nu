use ../../test.nu *

# Checking out an artifact must require its subtree: a principal without an artifact's subtree must not be able to check it out. An artifact the principal cannot access should be masked as not found.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a private file (artifact).
let path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let process = tg --token $alice.token build --detach $path | str trim
let file = (tg --token $alice.token wait $process | from json).output.value | split row '?' | first

# Eve does not have the subtree for Alice's private artifact.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not have the subtree for Alice's private artifact."

# Eve must not be able to check out an artifact whose subtree she does not have.
let checked_out = tg --token $eve.token checkout $file | complete
failure $checked_out "Eve must not check out an artifact whose subtree she does not have."
