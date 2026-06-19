use ../../test.nu *

# Touching a process must require read access: a principal that cannot read a process must not be able to touch it. An unreadable process should be masked as not found, otherwise touch is an existence oracle and lets any principal keep arbitrary processes alive against garbage collection.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process.
let path = artifact { tangram.ts: 'export default () => 5' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process | complete

# Eve cannot read Alice's private process.
let denied = tg --token $eve.token get $process | complete
failure $denied "Eve should not read Alice's private process."

# Eve must not be able to touch a process she cannot read; it should be masked as not found.
let touched = tg --token $eve.token process touch $process | complete
failure $touched "Eve must not touch a process she cannot read."
