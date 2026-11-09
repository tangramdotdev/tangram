use ../../util/test.nu *

# A watched checkin reflects an on-disk edit even when the watch cannot start its FSEvents stream. A single FSEvents stream accepts at most 4,096 paths, and a package that imports files from outside its root needs one path for the root and one for each imported file.

if $nu.os-info.name != 'macos' {
	skip_test 'this test requires macos'
}

let local = server spawn --config {
	cache: { map_size: 268_435_456 },
	index: { map_size: 268_435_456 },
}

# Create a package whose modules import 4,100 files from outside the package root. Each module imports at most 900 files, which keeps every file under the object children limit.
let path = artifact {
	external: {},
	package: {},
}
let external = $path | path join external
let package = $path | path join package
let count = 4100
for index in 0..<$count {
	$"($index)\n" | save ($external | path join $"($index).txt")
}
let modules = 0..<$count | each { |index| $index } | chunks 900 | enumerate | each { |chunk|
	let name = $"module($chunk.index).tg.ts"
	let imports = $chunk.item | each { |index| $"import \"../external/($index).txt\" with { type: \"file\" };" }
	$imports | append 'export const value = 1;' | str join "\n" | save ($package | path join $name)
	$name
}
let imports = $modules | each { |name| $"import \"./($name)\";" } | str join "\n"
$"($imports)\nexport default \(\) => \"one\";\n" | save ($package | path join tangram.ts)

# Check in the package with a watch.
let first = tg checkin --no-tokens $package --watch | referent node

# Edit the package's root module.
$"($imports)\nexport default \(\) => \"two\";\n" | save --force ($package | path join tangram.ts)

# A checkin without a watch reflects the edit.
let plain = tg checkin --no-tokens $package --no-watch | referent node
assert ($plain != $first) "the edit should change the checked in package"

# A watched checkin reflects the edit.
wait_until { (tg checkin --no-tokens $package --watch | referent node) == $plain } "the watched checkin should reflect the on-disk edit"
