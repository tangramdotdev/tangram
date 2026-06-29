use ../../test.nu *

# Publishing a process is bounded per field: a tagger who can read the node and output but not the log must not, by tagging --public, confer the log she cannot read.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let eve = tg login --verbose eve | from json

# Alice builds a process with an innocuous output and a secret in its log.
let path = artifact { tangram.ts: 'export default async function () { console.log("logsecret"); return tg.file("publicoutput"); }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process | complete
tg --token $alice.token index
let output = (tg --token $alice.token get $process | from json).output.value

# Alice grants Bob the node and the output, but not the log.
tg --token $alice.token grant $bob.user.id process_node $process | ignore
tg --token $alice.token grant $bob.user.id process_subtree_output $process | ignore

# Bob can read the node but not the log.
let bob_node = tg --token $bob.token get $process | complete
success $bob_node "Bob should read the process node he was granted."
let bob_log = tg --token $bob.token process log $process | complete
assert (not ($bob_log.stdout | str contains "logsecret")) "Bob should not read the log he was not granted."

# Bob publishes the process with a public tag; the recording is bounded by his fields.
tg --token $bob.token tag put bobproc $process --public
tg --token $bob.token index

# Eve resolves the public tag and reads the published node and output.
let eve_tag = tg --token $eve.token tag get bobproc | complete
success $eve_tag "Eve should resolve the public tag."
let eve_node = tg --token $eve.token get $process | complete
success $eve_node "Eve should read the published process node."
let eve_out = tg --token $eve.token get $output | complete
success $eve_out "Eve should read the published output Bob could read."

# But Eve must not read the log: Bob could not read it, so he could not publish it.
let eve_log = tg --token $eve.token process log $process | complete
assert (not ($eve_log.stdout | str contains "logsecret")) "a public process tag must not confer a field the tagger could not read; the log stays masked."
