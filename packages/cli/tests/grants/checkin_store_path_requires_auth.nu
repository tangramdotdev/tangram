use ../../test.nu *

# Checking in a store path must authorize the caller on the named artifact: the store-path branch mints an object subtree authorization token for the artifact ID in the path, so a principal who cannot read that artifact must not be able to check it in and obtain a read capability for it.

let dir = mktemp -d
let server = spawn --directory $dir --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice stores a private artifact; Eve cannot read it.
let secret = tg --token $alice.token put 'tg.file("topsecret-checkin")' | str trim
tg index
let before = tg --token $eve.token get $secret | complete
failure $before "Eve should not read Alice's private artifact before the exploit."

# Eve checks in the server's store path for Alice's artifact ID. This must be denied, since the store-path checkin mints an object-subtree authorization token for the ID without authorizing the caller.
let exploit = tg --token $eve.token checkin $"($dir)/store/($secret)" | complete
failure $exploit "Eve must not check in an artifact she cannot read and mint a read token for it."
