use ../../util/test.nu *

# A public build makes its process readable by another user only temporarily: once the permission ttl passes without a touch or a tag, the other user cannot read it.

let root_token = random chars
let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	process: { permission_ttl: 60 }
	scheduler: { runner_ttl: 300 }
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("temporary"); }' }

# Alice builds the command publicly, and Eve can read the process and its output.
let build = tg --token $alice.token build --detach --verbose --public $path | from json
let output = tg --token $alice.token wait $build.process | from json
let file = $output.output.value | split row '?' | first
tg --token $root_token index
success (tg --token $eve.token process get $build.process | complete) "Eve should read the public process"
assert equal (tg --token $eve.token cat $file | str trim) "temporary" "Eve should read the public output"

# Advance the clock past the permission ttl, and Eve can no longer read the process, while the process still exists.
advance_time $local 2min
tg --token $root_token index
failure (tg --token $eve.token process get $build.process | complete) "Eve should not read the process after its public permission expires"
success (tg --token $root_token process get $build.process | complete) "the process should still exist"
