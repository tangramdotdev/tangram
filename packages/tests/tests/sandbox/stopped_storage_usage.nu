use ../../util/test.nu *

# A stopped sandbox accrues storage usage for its directory until it starts again or is destroyed.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let local = server spawn --root --now '2026-01-01T00:00:00Z' --config {
	runner: {
		backend: { kind: builtin, max_filesystem_size: 67108864 },
		stopped_sandbox_interval: 1,
	},
}

# Stop a sandbox, whose directory holds its writable filesystem.
let sandbox = tg sandbox create --no-tokens | referent node
tg sandbox stop $sandbox
let stopped = tg sandbox get $sandbox | from json | get data | get usage?.storage?.consumption? | default 0

# Advance the clock while the sandbox is stopped, and start it, which reports the stopped storage usage until the start.
advance_time $local 5sec
tg sandbox start $sandbox
tg index
let started = tg sandbox get --source=index $sandbox | from json | get data | get usage?.storage?.consumption? | default 0
assert ($started > $stopped) "a stopped sandbox should accrue storage usage until it starts again"

# Stop the sandbox again, advance the clock, and destroy it, which reports the stopped storage usage until the destruction.
tg sandbox stop $sandbox
let stopped = tg sandbox get $sandbox | from json | get data | get usage?.storage?.consumption? | default 0
advance_time $local 5sec
tg sandbox destroy $sandbox
tg index
let destroyed = tg sandbox get $sandbox | from json | get data | get usage?.storage?.consumption? | default 0
assert ($destroyed > $stopped) "a stopped sandbox should accrue storage usage until it is destroyed"
