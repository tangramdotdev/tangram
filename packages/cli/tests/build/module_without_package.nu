use ../../test.nu *

# A standalone module file that is not part of a package can be built directly.

let server = spawn

let path = artifact {
	foo.tg.ts: '
		export default () => "Hello, World!";
	'
}

let output = tg build ($path | path join './foo.tg.ts')
snapshot $output
