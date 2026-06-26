use ../../test.nu *

# Touching an object must require read access: a principal that cannot read an object must not be able to touch it. An unreadable object should be masked as not found, otherwise touch is an existence oracle and lets any principal keep arbitrary objects alive against garbage collection.

let server = spawn --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private file (object).
let path = artifact { tangram.ts: 'export default function () { return tg.file("topsecret"); }' }
let process = tg --token $alice.token build --detach $path | str trim
let file = (tg --token $alice.token wait $process | from json).output.value.id

# Eve cannot read Alice's private object.
let denied = tg --token $eve.token get $file | complete
failure $denied "Eve should not read Alice's private object."

# Eve must not be able to touch an object she cannot read; it should be masked as not found.
let touched = tg --token $eve.token object touch $file | complete
failure $touched "Eve must not touch an object she cannot read."
