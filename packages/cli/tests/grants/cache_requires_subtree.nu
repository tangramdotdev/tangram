use ../../test.nu *

# Caching an artifact must require its subtree: a principal without an artifact's subtree must not be able to cache it. An artifact the principal cannot access should be masked as not found.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file (artifact).
let path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let process = tg --token $alice.token build --detach $path | str trim
let file = (tg --token $alice.token wait $process | from json).output.value.id

# Eve does not have the subtree for Alice's private artifact.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not have the subtree for Alice's private artifact."

# Eve must not be able to cache an artifact whose subtree she does not have.
let cached = tg --token $eve.token cache $file | complete
failure $cached "Eve must not cache an artifact whose subtree she does not have."
