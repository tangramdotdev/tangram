use ../../util/test.nu *

# Background cleaning does not remove an artifact's checkout from the store while a process in a sandbox uses it.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 60, ttt: 0 }
	vfs: false
}

let path = artifact {
	tangram.ts: '
		const input = tg.file("contents");
		const command = (arg: string) =>
			tg.command({
				args: ["-ec", `test -f "$INPUT"; echo ${arg}`],
				env: { INPUT: input },
				executable: "/bin/sh",
				host: tg.host.current,
			});
		export const first = () => command("first");
		export const second = () => command("second");
	'
}

# Run a process that reads the file, which checks the file out to the store.
let first = tg build $'($path)#first' | str trim
let output = tg run --sandbox $first | complete
success $output "the first process should read the file"

# Put an object that nothing references, to observe when cleaning collects what was created at this time.
let unreferenced = tg put --no-tokens 'tg.file("unreferenced")' | referent node
tg index

# Start a second process that reads the same file shortly before the ttl of the first process's checkout ends, and hold it before it runs.
advance_time $local 50sec
let second = tg build $'($path)#second' | str trim
let start = checkpoint watch runner.process.start
let process = tg run --sandbox --detach $second | referent node
checkpoint wait $start | ignore
tg index

# Advance past the ttl, and wait for cleaning to collect the unreferenced object.
let delete = checkpoint watch cleaning.object.delete --params { object: $unreferenced }
advance_time $local 20sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# Let the second process run. It should still be able to read the file.
checkpoint continue $start
checkpoint unwatch $start
let output = tg wait $process | from json
assert equal $output.exit 0 "the second process should read the file while cleaning runs"
