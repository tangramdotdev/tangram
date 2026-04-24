use ../../test.nu *

# Test that reading a blob from a remote-cached process output works
# without requiring a checkout to disk first.

let remote = spawn -n remote -u "http://localhost:8473"
let local = spawn -n local
tg remote put default $remote.url

# Inner module: produces a directory with log files (mimics proxy output).
let inner = artifact {
	tangram.ts: r#'
		export default () => tg.directory({
			build: tg.directory({
				"output.rmeta": tg.file("x".repeat(10000)),
			}),
			log: tg.directory({
				stdout: tg.file("expected stdout content"),
				stderr: tg.file(""),
			}),
		});
	'#
}

# Outer module: builds inner, then reads its log/stdout content.
# This mimics how the proxy reads logs from the process output.
let outer = mktemp -d
let tangram_ts = [
	$'import inner from "inner" with { local: "($inner)" };'
	''
	'export default async () => {'
	'	const dir = await tg.build(inner).then(tg.Directory.expect);'
	'	const stdout = await dir.get("log/stdout").then(tg.File.expect);'
	'	const content = await stdout.text;'
	'	return content;'
	'};'
] | str join "\n"
$tangram_ts | save ($outer | path join "tangram.ts")

# Build on the local server.
let process_id = tg build -d $outer | str trim
tg wait $process_id
tg index

# Push the inner build with its output.
let children = tg get $process_id | from json | get children | get item
for child in $children { tg push --eager --outputs $child }

# Start a fresh server with the remote.
let fresh = spawn -n fresh
tg remote put default $remote.url

# Build on the fresh server. The inner build is a cache hit.
# The outer module reads log/stdout from the inner build's output.
# This read should work without the inner output being checked out.
let output = tg build $outer
snapshot $output '"expected stdout content"'
