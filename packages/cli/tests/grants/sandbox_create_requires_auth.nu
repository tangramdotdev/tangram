use ../../test.nu *

# Creating a sandbox requires authentication when the server enforces it.

let server = spawn --config { authentication: true }

let denied = tg sandbox create --no-network | complete
failure $denied "an unauthenticated principal must not create a sandbox"

let alice = tg login --verbose alice | from json
let allowed = tg --token $alice.token sandbox create --no-network | complete
success $allowed "Alice should create a sandbox"
