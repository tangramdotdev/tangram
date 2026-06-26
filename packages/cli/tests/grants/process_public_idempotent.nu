use ../../test.nu *

# --public is idempotent: re-building a public command, and another owner building it, are cache hits that succeed without error rather than re-granting.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("x"); }' }

# Alice's first public build creates the public grant.
tg --token $alice.token build --detach --verbose --public $path | complete

# Alice re-builds --public (cache hit on her now-public process); must not error.
let b2 = tg --token $alice.token build --detach --verbose --public $path | complete
success $b2 "re-building --public must be idempotent."

# Bob --public builds the same command (cache hit on the public process); must not error.
let b3 = tg --token $bob.token build --detach --verbose --public $path | complete
success $b3 "another owner's --public on an already-public process must not error."
