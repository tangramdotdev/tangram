use ../../lib/test.nu *

# Building and checking out a package whose modules form a large import cycle must take time linear in the number of modules.

let local = server spawn

# Create a root module that imports every module, where every module imports the root module.
let count = 500
mut files = {}
mut root = ''
for i in 0..<$count {
	let name = 'module_' + ($i | into string) + '.tg.ts'
	$root = $root + 'import m' + ($i | into string) + ' from "./' + $name + '";' + "\n"
	$files = $files | insert $name ('import "./tangram.ts";' + "\n" + 'export default ' + ($i | into string) + ';' + "\n")
}
let sum = 0..<$count | each { |i| 'm' + ($i | into string) } | str join ' + '
$root = $root + 'export default () => ' + $sum + ';' + "\n"
$files = $files | insert 'tangram.ts' $root
let path = artifact $files

# Build the package.
let output = tg build $path | from json
assert equal $output (0..<$count | math sum)

# Check out the package.
let id = tg checkin --no-tokens $path
let checkout = $path | path dirname | path join 'checkout'
tg checkout $id --path $checkout | ignore
assert equal (ls $checkout | where name =~ 'module_' | length) $count
