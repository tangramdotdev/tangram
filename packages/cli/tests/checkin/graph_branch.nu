use ../lib/test.nu *

# Checking in a module cycle whose graph has more than 1,024 children splits the graph into leaves and a branch, and the modules can still be checked out and built.

let local = server spawn

# Create a cycle of two modules that each import 520 distinct files, so that the graph's nodes have more than 1,024 children.
let path = mktemp -d
for module in [a b] {
	let other = if $module == 'a' { 'b' } else { 'a' }
	let imports = 0..<520 | each { |i| $'import ($module)_($i) from "./($module)_($i).txt" with { type: "file" };' } | str join (char newline)
	let names = 0..<520 | each { |i| $'($module)_($i)' } | str join ', '
	$imports + (char newline) + $'import ($other) from "./($other).tg.ts";' + (char newline) + 'export let files = [' + $names + '];' + (char newline) | save ($path | path join $'($module).tg.ts')
	for i in 0..<520 {
		$'($module) ($i)' | save ($path | path join $'($module)_($i).txt')
	}
}
'import { files as a } from "./a.tg.ts";' + (char newline) + 'import { files as b } from "./b.tg.ts";' + (char newline) + 'export default async function () { let last = b[b.length - 1]; tg.File.assert(last); return `${a.length + b.length},${await last.text}`; }' + (char newline) | save ($path | path join tangram.ts)

# Check in the package.
let id = tg checkin --no-tokens $path | referent node

# The modules' graph is a branch whose leaves contain every node.
let file = tg get --no-tokens $'($id)?get=a.tg.ts'
let graph = $file | parse --regex '(?<graph>gph_[0-9a-z]+)' | get graph | first
let object = tg object get --no-tokens $graph
assert ($object | str contains '"children"') 'the graph must be a branch'
let counts = $object | parse --regex '"count":\s*(?<count>\d+)' | get count | into int
assert (($counts | length) > 1) 'the graph must have more than one leaf'
assert equal ($counts | math sum) 2 'the graph must contain every node'

# Check out the package and compare the modules.
let checkout = mktemp -d | path join checkout
tg checkout $id --path $checkout
assert equal (ls $checkout | length) 1_043 'the checkout must contain every file'
for name in [a.tg.ts b.tg.ts b_519.txt] {
	assert equal (open --raw ($checkout | path join $name)) (open --raw ($path | path join $name)) $'the checkout must have the correct contents of ($name)'
}

# Build the package, which loads both modules of the graph.
let output = tg build $path
assert equal $output '"1040,b 519"' 'the build must load every module'
