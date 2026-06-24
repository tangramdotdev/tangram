use ../../test.nu *

# Checking out an artifact to a path must require its subtree: a principal without an artifact's subtree must not be able to materialize it to disk. Otherwise checkout to a path is an exfiltration channel that bypasses the subtree check that cache enforces.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file (artifact).
let path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let process = tg --token $alice.token build --detach $path | str trim
let file = (tg --token $alice.token wait $process | from json).output.value.id

# Alice can check out her own artifact.
let alice_dir = mktemp --directory
let alice_out = $alice_dir | path join "out"
let alice_checkout = tg --token $alice.token checkout $file $alice_out | complete
success $alice_checkout "Alice should be able to check out her own artifact."

# Eve does not have the subtree for Alice's private artifact.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not have the subtree for Alice's private artifact."

# Eve must not be able to check out an artifact whose subtree she does not have.
let dir = mktemp --directory
let out = $dir | path join "out"
let checked_out = tg --token $eve.token checkout $file $out | complete
failure $checked_out "Eve must not check out an artifact whose subtree she does not have."
