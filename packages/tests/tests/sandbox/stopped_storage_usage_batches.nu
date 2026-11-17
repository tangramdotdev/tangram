use ../../util/test.nu *

# A runner reports its stopped sandboxes in batches, and each stopped sandbox accrues storage usage for its directory.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --root --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	runner: {
		backend: { kind: builtin, max_filesystem_size: 67108864 },
		stopped_sandbox_interval: 1,
		stopped_sandbox_report_batch_size: 2,
	},
}

# Stop five sandboxes, which take three batches to report.
let sandboxes = 0..4 | each {
	let sandbox = tg sandbox create --no-tokens | referent node
	tg sandbox stop --wait $sandbox
	$sandbox
}
let usage = {|sandbox| tg sandbox get $sandbox | from json | get data | get usage?.storage?.consumption? | default 0 }
let stopped = $sandboxes | each {|sandbox| do $usage $sandbox }

# Advance the clock, wait for the runner to report its stopped sandboxes, and await indexing. Every sandbox accrues storage usage.
let updated = checkpoint watch runner.sandbox.stopped.updated
advance_time $local 5sec
checkpoint pass $updated | ignore
checkpoint unwatch $updated
tg index
let pending = $sandboxes | zip $stopped | where {|pair| (do $usage $pair.0) <= $pair.1 } | length
assert equal $pending 0 "every stopped sandbox should accrue storage usage"
