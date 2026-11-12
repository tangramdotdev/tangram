use ../../util/test.nu *

# A build of a cached process reuses the cached process while a background index transaction is held open.

let local = server spawn --config {
	advanced: { checkpoints: true },
	process: { time_to_index: 0 },
}

let path = artifact {
	tangram.ts: '
		export default function () { return "hello"; }
	',
}

# Build the process once so that the second build is a cache hit, and wait for the index to have it.
let first = tg build --detach --no-tokens --verbose $path | from json
success (tg wait $first.process | complete)
tg index

# Hold the next background index transaction open by checking in an unrelated artifact.
let watch = tg checkpoint watch index.lmdb.write --params '{"priority":"medium"}' | from json | get watch
let other = artifact {
	file.txt: 'other',
}
tg checkin --no-tokens $other | ignore
tg checkpoint wait --timeout 10s index.lmdb.write $watch 0 | ignore

# The second build must reuse the cached process without waiting for the held transaction.
let output = timeout 10s tg build --detach --no-tokens --verbose $path | complete
tg checkpoint unwatch index.lmdb.write $watch
success $output "the cached build should not wait for the held index transaction"
let second = $output.stdout | from json
assert equal $second.process $first.process "the second build should reuse the cached process"
