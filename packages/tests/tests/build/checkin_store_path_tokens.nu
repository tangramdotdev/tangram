use ../../lib/test.nu *

# Checking in an artifact path reached through a symlink inside a process returns the existing artifact.

let local = server spawn --busybox
let file = tg put --no-tokens 'tg.file("checkin-store-path")' | referent node
let module = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async () => {
			const directory = await tg.directory({
				bin: { program: tg.file("checkin-store-path") },
			});
			return tg.build`
				path="\${TMPDIR:-/tmp}/checkin-alias"
				ln -s ${directory} "$path"
				tg checkin --no-tokens "$path/bin/../bin/program" > ${tg.output}
			`.env(tg.build(busybox));
		};
	'
}

let process = tg build --no-tokens --detach $module | referent node
let result = timeout 30s tg wait $process | from json
if $result.exit != 0 {
	tg get --depth inf $result.error
	let children = tg children $process | from json
	for child in ($children | where $it starts-with pcs_) {
		tg log $child
	}
}
assert equal $result.exit 0
let output = tg read $result.output.value
assert equal ($output | referent node) $file
