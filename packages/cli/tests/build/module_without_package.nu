use ../lib/test.nu *

# A standalone module file that is not part of a package can be built directly.

let local = server spawn

let path = artifact {
	foo.tg.ts: '
		export default function () { return "Hello, World!"; }
	'
}

let output = tg build ($path | path join './foo.tg.ts')
snapshot $output
