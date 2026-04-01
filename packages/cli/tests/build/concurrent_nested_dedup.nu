use ../../test.nu *

# Test that many concurrent builds sharing a deeply nested dependency
# chain are correctly deduplicated.
#
# 50 concurrent tg.build calls each trigger a 4-level deep chain of
# tg.build calls to the same shared modules. The server must deduplicate
# the nested builds correctly without cancellation errors.

let server = spawn

# Level 4 (deepest).
let level4 = artifact {
	tangram.ts: '
		export default () => tg.directory({ "tool": tg.file("built tool") });
	'
}

# Level 3: calls level 4.
let level3 = mktemp -d
$'import level4 from "level4" with { local: "($level4)" };
export default async () => tg.build(level4).then(tg.Directory.expect);
' | save ($level3 | path join "tangram.ts")

# Level 2: calls level 3.
let level2 = mktemp -d
$'import level3 from "level3" with { local: "($level3)" };
export default async () => {
	const dir = await tg.build(level3).then(tg.Directory.expect);
	return dir.get("tool").then(tg.File.expect);
};
' | save ($level2 | path join "tangram.ts")

# Level 1: calls level 2.
let level1 = mktemp -d
$'import level2 from "level2" with { local: "($level2)" };
export default async () => tg.build(level2).then(tg.File.expect);
' | save ($level1 | path join "tangram.ts")

# Outer: 50 concurrent tg.build(wrapper) calls that each trigger the full chain.
let outer = mktemp -d
let count = 50
let builds = 0..<$count | each { |i|
	'tg.build(wrapper, "item' + ($i | into string) + '").then(tg.File.expect)'
} | str join ", "
let outer_ts = [
	$'import level1 from "level1" with { local: "($level1)" };'
	''
	'const wrapper = async (name) => {'
	'	const tool = await tg.build(level1).then(tg.File.expect);'
	'	return tg.file(name + ": " + await tool.text);'
	'};'
	''
	'export default async () => {'
	('	const results = await Promise.all([' + $builds + ']);')
	'	return tg.directory(Object.fromEntries(results.map((f, i) => [`r${i}`, f])));'
	'};'
] | str join "\n"
$outer_ts | save ($outer | path join "tangram.ts")

let output = tg build $outer
snapshot $output
