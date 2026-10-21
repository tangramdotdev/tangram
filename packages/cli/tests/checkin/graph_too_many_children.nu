use ../lib/test.nu *

# Checking in a module cycle whose graph has more than 1,024 children fails with an error that names the limit.

let local = server spawn

# Create a cycle of 1,026 modules with distinct contents.
let path = mktemp -d
let modules = 0..<1_025
$modules | each { |i| $'import "./module_($i).tg.ts";' } | str join (char newline) | save ($path | path join 'tangram.ts')
for i in $modules {
	$'import "./tangram.ts";(char newline)export let value = ($i);' | save ($path | path join $'module_($i).tg.ts')
}

let output = tg checkin --no-tokens $path | complete
failure $output 'the checkin must be rejected'
assert ($output.stderr | str contains 'the object has too many children') 'the error must name the children limit'
