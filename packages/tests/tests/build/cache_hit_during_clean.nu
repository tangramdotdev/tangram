use ../../util/test.nu *

# A cache lookup must reject a process that cleaning deletes after it was selected.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	indexer: { cleaning: false },
	process: { time_to_index: 0 },
}
let path = artifact { tangram.ts: 'export default () => "hello";' }
let first = tg build --detach --no-tokens --verbose $path | from json
success (tg wait $first.process | complete)
tg index

# Clean the sandbox while retaining the process, then make the process eligible for the next clean.
tg tag retained $first.process
tg clean
tg tag delete retained
tg index
advance_time $local 1sec

# Hold the cache lookup after it selects the process.
let touch = checkpoint watch process.spawn.cached.touch --params { process: $first.process }
let build = job spawn {
	let id = job id
	tg build --cached=true --detach --no-tokens --verbose $path | complete | job send --tag $id 0
}
checkpoint wait $touch | ignore

# Hold cleaning before it deletes the process, then let the lookup try to touch it.
let cleaning = checkpoint watch index.clean
let clean = job spawn {
	let id = job id
	tg clean | complete | job send --tag $id 0
}
checkpoint wait $cleaning | ignore
checkpoint unwatch $touch
let early = try { job recv --tag $build --timeout 1sec } catch { null }
checkpoint unwatch $cleaning
success (job recv --tag $clean --timeout 10sec)
assert equal $early null "the lookup must wait for cleaning before accepting the cached process"

# The cache-only build must report a miss instead of returning a deleted process.
failure (job recv --tag $build --timeout 10sec) "the cleaned process must not be returned as a cache hit"
failure (tg process get $first.process | complete) "the process should have been cleaned"
