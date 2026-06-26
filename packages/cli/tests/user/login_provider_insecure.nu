use ../../test.nu *

# The insecure login provider can be selected implicitly or explicitly when it is configured.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let implicit = tg login --verbose alice | from json
assert equal $implicit.user.name alice

let explicit = tg login --provider insecure --verbose bob | from json
assert equal $explicit.user.name bob
