use ../../test.nu *

# Usage is available through the user-specific and top-level commands.

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json

let initial = tg usage | from json
assert ($initial.sandbox_cpu == 0)
assert ($initial.sandbox_memory == 0)
assert ($initial.object_count == 0)
assert ($initial.object_size == 0)
assert ($initial.process_count == 0)
assert ($initial.period.kind == "month")

let old = tg usage --day 1970-01-01 | complete
failure $old "usage before tracking started should be unavailable"
assert ($old.stderr | str contains "usage is unavailable for the requested period")

tg put 'tg.file("hello")'
tg index

let top_level = tg usage $alice.user.id | from json
let user = tg user usage | from json
assert ($top_level == $user)
assert ($user.object_count >= 1)
assert ($user.object_size > 0)
