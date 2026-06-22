use ../../test.nu *

# Creating a sandbox must require authentication when the server enforces it: an unauthenticated principal must not be able to create a sandbox, which would be unowned (no creator, no grant) and could consume host resources.

let server = spawn --config { authentication: true }

# An unauthenticated principal must not be able to create a sandbox.
let denied = tg sandbox create --no-network | complete
failure $denied "An unauthenticated principal must not create a sandbox."

# An authenticated principal can create a sandbox.
let alice = tg login --verbose alice | from json
let allowed = tg --token $alice.token sandbox create --no-network | complete
success $allowed "Alice should be able to create a sandbox."
