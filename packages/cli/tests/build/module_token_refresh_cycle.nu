use ../../test.nu *

# A broad cyclic module graph builds successfully when module resolution refreshes authorization tokens.

let server = server spawn

let path = mktemp -d
let modules = 0..<200
let imports = $modules
	| each { |index| $'import "./module_($index).tg.ts";' }
	| str join (char newline)
[$imports 'export default function () { return "Hello, World!"; }']
	| str join (char newline)
	| save ($path | path join 'tangram.ts')
for index in $modules {
	'import "./tangram.ts";' | save ($path | path join $'module_($index).tg.ts')
}

let output = timeout 15s tg build $path | complete
success $output
