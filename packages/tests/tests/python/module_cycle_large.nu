use ../../lib/test.nu *

# Building a Python package whose modules form a large import cycle must take time linear in the number of modules.

let local = server spawn

# Create a root module that imports every module, where the first module imports the second and every other module imports the first, so that the modules form a cycle without deeply nested imports.
let count = 500
mut files = {}
for i in 0..<$count {
	let next = if $i == 0 { 1 } else { 0 }
	let name = 'module_' + ($i | into string) + '.tg.py'
	$files = $files | insert $name ('from . import module_' + ($next | into string) + "\n" + 'value = ' + ($i | into string) + "\n")
}
let names = 0..<$count | each { |i| 'module_' + ($i | into string) } | str join ', '
$files = $files | insert 'tangram.py' ('from . import ' + $names + "\n" + 'def default():' + "\n" + '    return sum(module.value for module in [' + $names + '])' + "\n")
let path = artifact $files

# Build the package.
let output = tg build ($path | path join tangram.py) | from json
assert equal $output (0..<$count | math sum)
