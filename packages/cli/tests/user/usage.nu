use ../../test.nu *

# Usage is available through the user-specific and top-level commands.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json

let initial = tg usage | from json
assert ($initial.object_count == 0)
assert ($initial.object_size == 0)
assert ($initial.process_count == 0)

tg put 'tg.file("hello")'

let top_level = tg usage $alice.user.id | from json
let user = tg user usage | from json
assert ($top_level == $user)
assert ($user.object_count >= 1)
assert ($user.object_size > 0)
