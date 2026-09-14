use ../../test.nu *

# Checkin reuses the command's artifact tokens inside a process, including without the VFS.

let server = server spawn --busybox --config { advanced: { checkpoints: true } }
let file = tg put 'tg.file("checkin-store-path")' | str trim
let bin = tg put 'tg.directory({ "program": tg.file("checkin-store-path") })' | str trim
let directory = tg put 'tg.directory({ "bin": tg.directory({ "program": tg.file("checkin-store-path") }) })' | str trim
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
				tg checkin "$path/bin/../bin/program" > ${tg.output}
			`.env(tg.build(busybox));
		};
	'
}

# Allow setup to finish, then block authorization index searches for every artifact in the path.
let start_watch = tg checkpoint watch runner.process.start | from json | get watch
let process = tg build --detach $module | str trim
timeout 30s tg checkpoint wait runner.process.start $start_watch 0 | ignore
let watches = [$directory $bin $file] | each { |id|
	let params = { resource: $id } | to json --raw
	tg checkpoint watch authorization.index --params $params | from json | get watch
}
tg checkpoint continue runner.process.start $start_watch 0
tg checkpoint unwatch runner.process.start $start_watch
let result = timeout 30s tg wait $process | from json
if $result.exit != 0 {
	tg get --depth inf $result.error
	let children = tg children $process | from json
	for child in ($children | where $it starts-with pcs_) {
		tg log $child
	}
}
assert equal $result.exit 0
for watch in $watches {
	tg checkpoint unwatch authorization.index $watch
}
let output = tg read $result.output.value
assert equal ($output | str trim) $file
