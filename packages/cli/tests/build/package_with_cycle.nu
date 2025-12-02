use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import "./foo.tg.ts"
		export default () => "Hello, World!";
	'
	foo.tg.ts: '
		import "./tangram.ts"
	'
}

let output = tg build $path | complete
success $output
