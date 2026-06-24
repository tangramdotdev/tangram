use ../../test.nu *

# A non-root user must not create a sandbox owned by root.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

# Alice cannot act as root, so she must not claim root as a sandbox owner.
let create = tg --token $alice.token sandbox create --owner root --no-network | complete
failure $create "Alice must not create a sandbox owned by root"
