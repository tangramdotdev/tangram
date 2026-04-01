use ../../test.nu *

# Test that concurrent builds with deeply nested tg.build chains correctly
# use remote cache hits on a fresh server.

let remote = spawn -n remote -u "http://localhost:8473"
let local = spawn -n local
tg remote put default $remote.url

# Level 4 (deepest). Uses tg.sleep so the process takes real time,
# giving the watchdog a window to cancel it after all callers' remotes win.
let level4 = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(1);
			return tg.directory({ "tool": tg.file("built tool") });
		};
	'
}

# Level 3: calls level 4.
let level3 = mktemp -d
let level3_ts = [
	$'import level4 from "level4" with { local: "($level4)" };'
	'export default async () => tg.build(level4).then(tg.Directory.expect);'
] | str join "\n"
$level3_ts | save ($level3 | path join "tangram.ts")

# Level 2: calls level 3.
let level2 = mktemp -d
let level2_ts = [
	$'import level3 from "level3" with { local: "($level3)" };'
	'export default async () => {'
	'	const dir = await tg.build(level3).then(tg.Directory.expect);'
	'	return dir.get("tool").then(tg.File.expect);'
	'};'
] | str join "\n"
$level2_ts | save ($level2 | path join "tangram.ts")

# Level 1: calls level 2.
let level1 = mktemp -d
let level1_ts = [
	$'import level2 from "level2" with { local: "($level2)" };'
	'export default async () => tg.build(level2).then(tg.File.expect);'
] | str join "\n"
$level1_ts | save ($level1 | path join "tangram.ts")

# Wrapper module: calls level 1, produces a result per name.
let wrapper = mktemp -d
let wrapper_ts = [
	$'import level1 from "level1" with { local: "($level1)" };'
	'export default async (name) => {'
	'	const tool = await tg.build(level1).then(tg.File.expect);'
	'	return tg.file(name + ": " + await tool.text);'
	'};'
] | str join "\n"
$wrapper_ts | save ($wrapper | path join "tangram.ts")

# Outer module: 50 concurrent tg.build(wrapper, name) calls.
let outer = mktemp -d
let count = 50
let builds = 0..<$count | each { |i|
	'tg.build(wrap, "item' + ($i | into string) + '").then(tg.File.expect)'
} | str join ", "
let outer_ts = [
	$'import wrap from "wrap" with { local: "($wrapper)" };'
	''
	'export default async () => {'
	('	const results = await Promise.all([' + $builds + ']);')
	'	return tg.directory(Object.fromEntries(results.map((f, i) => [`r${i}`, f])));'
	'};'
] | str join "\n"
$outer_ts | save ($outer | path join "tangram.ts")

# Build on the local server. This must succeed.
let id = tg build $outer

# Push everything to the remote.
tg push --eager --outputs --recursive $id

# Fresh server with the remote configured.
let fresh = spawn -n fresh
tg remote put default $remote.url

# Rebuild on the fresh server. All nested builds should be remote cache hits.
let fresh_id = tg build $outer
snapshot $fresh_id $id
