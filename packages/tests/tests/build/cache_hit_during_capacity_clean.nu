use ../../util/test.nu *

# Capacity cleaning must not let a cache lookup return a process that it deletes before the configured ttl.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	indexer: {
		cleaning: {
			capacity: { kind: bytes, start_above: 1, stop_at: 0 },
			poll_interval: 60,
		},
	},
	process: { time_to_index: 0 },
	scheduler: { runner_ttl: 300 },
}

# Hold the cleaner with a cutoff before the process is built.
let cleaning = checkpoint watch index.clean
advance_time $local 1min
checkpoint wait $cleaning | ignore
advance_time $local 1sec
let path = artifact { tangram.ts: 'export default () => "hello";' }
let first = tg build --detach --no-tokens --verbose $path | from json
success (tg wait $first.process | complete)
tg index
tg tag retained $first.process
checkpoint unwatch $cleaning

# Clean the sandbox while retaining the process, then release the process for the next capacity clean.
tg clean
tg tag delete retained
tg index

# Hold the cache lookup after it selects the process.
let touch = checkpoint watch process.spawn.cached.touch --params { process: $first.process }
let build = job spawn {
	let id = job id
	tg build --cached=true --detach --no-tokens --verbose $path | complete | job send --tag $id 0
}
checkpoint wait $touch | ignore

# Wake capacity cleaning and hold it before it deletes the process.
let cleaning = checkpoint watch index.clean
let deleted = checkpoint watch cleaning.process.delete --params { process: $first.process }
advance_time $local 1min
checkpoint wait $cleaning | ignore
checkpoint unwatch $touch
let early = try { job recv --tag $build --timeout 1sec } catch { null }
checkpoint unwatch $cleaning
checkpoint pass $deleted | ignore
checkpoint unwatch $deleted
assert equal $early null "the lookup must wait for capacity cleaning before accepting the cached process"

# The cache-only build must report a miss even though the process's configured ttl has not expired.
failure (job recv --tag $build --timeout 10sec) "the capacity-cleaned process must not be returned as a cache hit"
failure (tg process get $first.process | complete)
