use ../../util/test.nu *

# Cleaning waits for a pending cache touch before it can collect the cached process.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	indexer: { cleaning: false },
	process: { time_to_index: 0, ttt: 0 },
}
let path = artifact { tangram.ts: 'export default () => "hello";' }
let first = tg build --detach --no-tokens --verbose $path | from json
success (tg wait $first.process | complete)
tg index

# Remove the sandbox while retaining the process, then make the process eligible for cleaning.
tg tag retained $first.process
tg clean
tg tag delete retained
tg index
advance_time $local 1sec

# Return the cache hit while its touch is held before the write.
let touch = checkpoint watch process.spawn.cached.touch.write --params { process: $first.process }
let second = tg build --detach --no-tokens --verbose $path | from json
assert equal $second.process $first.process
checkpoint wait $touch | ignore

# Cleaning must wait while the cached process is still being touched.
let clean = job spawn {
	let id = job id
	tg clean | complete | job send --tag $id 0
}
let early = try { job recv --tag $clean --timeout 1sec } catch { null }
let output = tg wait $second.process | complete
checkpoint unwatch $touch
assert equal $early null "cleaning must wait for the pending cache touch"
success $output "the returned cached process must remain readable while its touch is pending"
success (job recv --tag $clean --timeout 10sec)
