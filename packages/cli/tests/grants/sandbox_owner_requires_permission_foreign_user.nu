use ../../test.nu *

# Creating a sandbox owned by another user requires write on that user, so an unrelated user must not assign them as owner.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

# Alice has no write on Bob, so she must not be able to assign Bob as the owner of a new sandbox.
let create = tg --token $alice.token sandbox create --owner $bob.user.id --no-network | complete
failure $create "Alice must not create a sandbox owned by a user she cannot write"
