use ../../test.nu *

# Triggering a server-wide clean must be privileged: an ordinary user must not be able to garbage-collect another user's data. The clean handler has no authorization gate, so any authenticated user can evict everyone's un-pinned objects.

let server = spawn --config { authentication: { providers: { insecure: true } }, advanced: { single_process: true } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice stores an object and can read it.
let obj = tg --token $alice.token put 'tg.file("alice-keep")' | str trim
tg index
let before = tg --token $alice.token get $obj | complete
success $before "Alice should read her own object before the clean."

# Eve triggers a clean. This must not be allowed to destroy Alice's data.
tg --token $eve.token clean | complete

# Alice's object must survive: Eve must not be able to garbage-collect another user's data.
let after = tg --token $alice.token get $obj | complete
success $after "Alice's object must survive a clean triggered by another user."
