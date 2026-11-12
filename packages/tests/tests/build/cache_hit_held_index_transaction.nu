use ../../util/test.nu *

# A cached build skips a touch within its touch interval and waits for a touch when the interval expires.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	process: { time_to_index: 0, ttt: 2 },
}

# Build the process and wait for the index to have it.
let path = artifact { tangram.ts: 'export default () => "hello";' }
let first = tg build --detach --no-tokens --verbose $path | from json
success (tg wait $first.process | complete)
tg index
advance_time $local 1sec

# Hold the next background index transaction open by checking in an unrelated artifact.
let watch = checkpoint watch index.lmdb.write --params { priority: medium }
let other = artifact { file.txt: 'other' }
tg checkin --no-tokens $other | ignore
checkpoint wait $watch | ignore

# Within time_to_touch, the cache hit must not wait for the writer.
let output = timeout 10s tg build --cached=true --detach --no-tokens --verbose $path | complete
checkpoint unwatch $watch
success $output "the cached build should not wait for an unnecessary touch"
assert equal ($output.stdout | from json | get process) $first.process

# At time_to_touch, the cache hit must wait for its touch to finish.
advance_time $local 1sec
let watch = checkpoint watch index.lmdb.write --params { priority: medium }
let other = artifact { file.txt: 'another' }
tg checkin --no-tokens $other | ignore
checkpoint wait $watch | ignore
let build = job spawn {
	let id = job id
	tg build --cached=true --detach --no-tokens --verbose $path | complete | job send --tag $id 0
}
let early = try { job recv --tag $build --timeout 1sec } catch { null }
checkpoint unwatch $watch
assert equal $early null "the cached build should wait for a due touch"
let output = job recv --tag $build --timeout 10sec
success $output
assert equal ($output.stdout | from json | get process) $first.process
