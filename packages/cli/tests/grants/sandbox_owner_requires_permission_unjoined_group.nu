use ../../test.nu *

# Creating a sandbox owned by a group requires write on that group, so read access to it is not enough to claim it as owner.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json

# Bob owns a group and grants Alice only read on it, so Alice can resolve it but cannot write it.
tg --token $bob.token group create secret
tg --token $bob.token grant $alice.user.id read secret

# Read on the group is not enough to claim it as a sandbox owner.
let create = tg --token $alice.token sandbox create --owner secret --no-network | complete
failure $create "Alice must not create a sandbox owned by a group she can only read"
