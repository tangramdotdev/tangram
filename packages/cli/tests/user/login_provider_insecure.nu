use ../../test.nu *

# The insecure login provider is selected implicitly when it is configured.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let user = tg login --verbose alice | from json
assert equal $user.user.name alice
