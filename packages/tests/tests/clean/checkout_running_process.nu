use ../../util/test.nu *

# Background cleaning does not remove an artifact's checkout from the store while a process that checked it out more than the ttl ago still uses it.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
}

let path = artifact {
	tangram.ts: '
		export default () =>
			tg.command({
				args: ["-ec", `test -f "$INPUT"`],
				env: { INPUT: tg.file("contents") },
				executable: "/bin/sh",
				host: tg.host.current,
			});
	'
}

# Start a process that reads a file, and hold it after it checks the file out and before it runs.
let command = tg build $path | str trim
let start = checkpoint watch runner.process.start
let process = tg run --sandbox --detach $command | referent node
checkpoint wait $start | ignore

# Put an object that nothing references, to observe when cleaning collects what was created at this time.
let unreferenced = tg put --no-tokens 'tg.file("unreferenced")' | referent node
tg index

# Advance past the ttl while the process is held, and wait for cleaning to collect the unreferenced object.
let delete = checkpoint watch cleaning.object.delete --params { object: $unreferenced }
advance_time $local 20sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# Let the process run. It should still be able to read the file.
checkpoint continue $start
checkpoint unwatch $start
let output = tg wait $process | from json
assert equal $output.exit 0 "the process should read the file after cleaning runs"
