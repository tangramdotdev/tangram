use ../../test.nu *

let server = spawn

let path = artifact {
	foo.tg.ts: '
		export default () => "Hello, World!";
	'
}

let output = run tg build ($path | path join './foo.tg.ts')
snapshot $output
