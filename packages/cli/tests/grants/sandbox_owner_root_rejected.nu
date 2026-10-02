use ../lib/test.nu *

# A non-root user must not create a sandbox owned by root.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json

# Alice cannot act as root, so she must not claim root as a sandbox owner.
let create = tg --token $alice.token sandbox create --no-tokens --owner root --no-network | complete
failure $create "Alice must not create a sandbox owned by root"
