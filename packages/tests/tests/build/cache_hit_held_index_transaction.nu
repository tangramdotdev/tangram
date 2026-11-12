use ../../util/test.nu *

# A build of a cached process reuses the cached process while a background index transaction is held open.

let local = server spawn --config {
	advanced: { checkpoints: true },
	index: { write_channel_capacity: 1 },
	indexer: { cleaning: false },
	process: { time_to_index: 0 },
}

let path = artifact {
	tangram.ts: '
		export function a() { return "a"; }
		export function b() { return "b"; }
		export function c() { return "c"; }
	',
}

# Build each export so that the next builds are cache hits, and wait for the index to have them.
let first = [a b c] | each { |export|
	let reference = $'($path)#($export)'
	let build = tg build --detach --no-tokens --verbose $reference | from json
	success (tg wait $build.process | complete)
	{ reference: $reference, process: $build.process }
}
tg index

# Hold the next background index transaction open by checking in an unrelated artifact.
let watch = tg checkpoint watch index.lmdb.write --params '{"priority":"medium"}' | from json | get watch
let other = artifact {
	file.txt: 'other',
}
tg checkin --no-tokens $other | ignore
tg checkpoint wait --timeout 10s index.lmdb.write $watch 0 | ignore

# Repeated cache hits must stay responsive even when their touches fill the writer queue.
let outputs = $first | each { |build|
	let outputs = 0..<2 | each {
		timeout 10s tg build --detach --no-tokens --verbose $build.reference | complete
	}
	{ expected: $build.process, outputs: $outputs }
}
tg checkpoint unwatch index.lmdb.write $watch
for build in $outputs {
	for output in $build.outputs {
		success $output "the cached build should not wait for the held index transaction"
		let second = $output.stdout | from json
		assert equal $second.process $build.expected "the build should reuse the cached process"
	}
}
