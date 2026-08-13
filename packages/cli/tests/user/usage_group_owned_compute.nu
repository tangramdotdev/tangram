use ../../test.nu *

# Compute for a group-owned sandbox is charged to the group's user account.

let server = spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
tg --token $alice.token group create alice/team

let before = tg --token $alice.token usage | from json
let path = artifact { tangram.ts: 'export default () => tg.file("hello")' }
let process = tg --token $alice.token build --detach --owner alice/team $path | str trim
tg --token $alice.token wait $process
let after = tg --token $alice.token usage | from json

assert ($after.sandbox_cpu > $before.sandbox_cpu) "a group-owned sandbox must charge CPU usage"
assert ($after.sandbox_memory > $before.sandbox_memory) "a group-owned sandbox must charge memory usage"
assert ($after.sandbox_count > $before.sandbox_count) "a group-owned sandbox must charge sandbox usage"
