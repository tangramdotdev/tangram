use ../../test.nu *

# A principal who cannot read a private file must not be able to checksum it, since checksumming reads its bytes.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice stores a private file; Eve cannot read it.
let secret = tg --token $alice.token put 'tg.file("checksumsecret")' | str trim
tg index
let before = tg --token $eve.token get $secret | complete
failure $before "Eve should not read Alice's private file before the exploit."

# Eve must not be able to checksum a file she cannot read.
let exploit = tg --token $eve.token checksum $secret | complete
failure $exploit "Eve must not checksum a private file she cannot read."
