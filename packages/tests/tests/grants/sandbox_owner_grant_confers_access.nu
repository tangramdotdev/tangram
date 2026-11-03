use ../../util/test.nu *

# A write grant on the owning group confers control of the sandbox, and a read grant confers only read access.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let dave = tg login --verbose --name dave | from json

tg --token $alice.token group create team
tg --token $alice.token grant $bob.user.id write team
let sandbox = tg --token $bob.token sandbox create --no-tokens --group team --no-network | referent node

# A write grant on the owner confers access.
tg --token $alice.token grant $carol.user.id write team
success (tg --token $carol.token sandbox get $sandbox | complete) "a write grant on the owner should confer sandbox access"

# A read grant on the owner confers read access but not control.
tg --token $alice.token grant $dave.user.id read team
success (tg --token $dave.token sandbox get $sandbox | complete) "a read grant on the owner should confer read access to the sandbox"
failure (tg --token $dave.token sandbox destroy $sandbox | complete) "a read grant on the owner must not allow destroying the sandbox"

tg --token $bob.token sandbox destroy $sandbox
