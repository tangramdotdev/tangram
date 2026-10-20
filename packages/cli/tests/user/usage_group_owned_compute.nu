use ../lib/test.nu *

# Compute for a group-owned sandbox is charged to the group's user account.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
tg --token $alice.token group create alice/team

let before = tg --token $alice.token usage | from json
let path = artifact { tangram.ts: 'export default () => tg.file("hello")' }
let process = tg --token $alice.token build --no-tokens --detach --owner alice/team $path | referent node
tg --token $alice.token wait $process
let sandbox = tg --token $alice.token process get $process | from json | get sandbox
tg --token $alice.token wait $sandbox
tg index
let after = tg --token $alice.token usage | from json

assert ($after.sandbox_count > $before.sandbox_count) "a group-owned sandbox must charge sandbox usage"

# A darwin sandbox reports no resource usage, so only a linux sandbox charges CPU and memory.
if $nu.os-info.name == 'linux' {
	assert ($after.sandbox_cpu_consumption > $before.sandbox_cpu_consumption) "a group-owned sandbox must charge CPU usage"
	assert ($after.sandbox_memory_consumption > $before.sandbox_memory_consumption) "a group-owned sandbox must charge memory usage"
}
