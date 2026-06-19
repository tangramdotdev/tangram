use ../../test.nu *

# Caching an artifact must require read access: a principal that cannot read an artifact must not be able to cache it. An unreadable artifact should be masked as not found, otherwise cache is an existence oracle and lets any principal force the server to materialize and pull arbitrary artifacts.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file (artifact).
let path = artifact { tangram.ts: 'export default () => tg.file("topsecret")' }
let process = tg --token $alice.token build --detach $path | str trim
let file = (tg --token $alice.token wait $process | from json).output.value

# Eve cannot read Alice's private artifact.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private artifact."

# Eve must not be able to cache an artifact she cannot read.
let cached = tg --token $eve.token cache $file | complete
failure $cached "Eve must not cache an artifact she cannot read."
