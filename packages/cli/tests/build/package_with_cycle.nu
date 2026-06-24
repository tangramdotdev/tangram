use ../../test.nu *

# A package whose modules import each other in a cycle builds successfully.

let server = spawn

let path = artifact {
	tangram.ts: '
		import "./foo.tg.ts"
		export default function () { return "Hello, World!"; }
	'
	foo.tg.ts: '
		import "./tangram.ts"
	'
}

let output = tg build $path | complete
success $output
