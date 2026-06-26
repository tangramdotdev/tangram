use ../../test.nu *

# At most one of the owner, user, group, and organization options may be provided.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json

# Providing two owner selectors at once is a usage error.
failure (tg --token $alice.token sandbox create --owner team --group team --no-network | complete) "--owner and --group must conflict"
failure (tg --token $alice.token sandbox create --group team --organization team --no-network | complete) "--group and --organization must conflict"
failure (tg --token $alice.token sandbox create --org team --user team --no-network | complete) "--org and --user must conflict"
