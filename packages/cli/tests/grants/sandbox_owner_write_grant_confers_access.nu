use ../../test.nu *

# A write grant on the owning group confers sandbox access, but a read-only grant on the owner does not.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let bob = tg login --verbose bob | from json
let carol = tg login --verbose carol | from json
let dave = tg login --verbose dave | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
let sandbox = tg --token $bob.token sandbox create --group team --no-network | str trim

# A write grant on the owner confers access.
tg --token $alice.token grant $carol.user.id write team
success (tg --token $carol.token sandbox get $sandbox | complete) "a write grant on the owner should confer sandbox access"

# A read grant on the owner does not confer access; the owner dependency requires write.
tg --token $alice.token grant $dave.user.id read team
failure (tg --token $dave.token sandbox get $sandbox | complete) "a read-only grant on the owner must not confer sandbox access"
