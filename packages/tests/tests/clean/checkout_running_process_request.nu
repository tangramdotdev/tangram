use ../../util/test.nu *

# Background cleaning does not remove a checkout that a running process requested more than the ttl ago.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
}

let path = artifact {
	tangram.ts: '
		export default async () => {
			const directory = await tg.directory({ first: tg.file("first"), second: tg.file("second") });
			const first = await directory.get("first");
			const second = await directory.get("second");
			return tg.command({
				args: ["-ec", `tg checkout ${first.id} > /dev/null; tg checkout ${second.id} > /dev/null; test -f "\${INPUT%/*}/${first.id}"`],
				env: { INPUT: directory },
				executable: "/bin/sh",
				host: tg.host.current,
			});
		};
	'
}

# Start a process that checks out one file, and hold it when it checks out another.
let command = tg build $path | str trim
let second = tg put --no-tokens 'tg.file("second")' | referent node
let unindexed = checkpoint watch checkout.artifact.unindexed --params { artifact: $second }
let process = tg run --sandbox --detach $command | referent node
checkpoint wait $unindexed | ignore

# Put an object that nothing references, to observe when cleaning collects what was created at this time.
let unreferenced = tg put --no-tokens 'tg.file("unreferenced")' | referent node
tg index

# Advance past the ttl while the process is held, and wait for cleaning to collect the unreferenced object.
let delete = checkpoint watch cleaning.object.delete --params { object: $unreferenced }
advance_time $local 20sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# Let the process continue. The first file's checkout should still exist.
checkpoint unwatch $unindexed
let output = tg wait $process | from json
assert equal $output.exit 0 "the process should find the first checkout after cleaning runs"
server stop $local
