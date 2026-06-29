use ../../test.nu *

# Checking in a cache path must authorize the caller on the named artifact: the cache-path branch mints an object subtree grant token for the artifact id in the path, so a principal who cannot read that artifact must not be able to check it in and obtain a read capability for it.

let dir = mktemp -d
let server = spawn --directory $dir --config { authentication: { providers: { insecure: true } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice stores a private artifact; Eve cannot read it.
let secret = tg --token $alice.token put 'tg.file("topsecret-checkin")' | str trim
tg index
let before = tg --token $eve.token get $secret | complete
failure $before "Eve should not read Alice's private artifact before the exploit."

# Eve checks in the server's cache path for Alice's artifact id. This must be denied, since
# checkin_cache_path mints an object_subtree grant token for the id without authorizing the caller.
let exploit = tg --token $eve.token checkin $"($dir)/artifacts/($secret)" | complete
failure $exploit "Eve must not check in an artifact she cannot read and mint a read token for it."
