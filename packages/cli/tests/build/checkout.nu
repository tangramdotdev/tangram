use ../lib/test.nu *

# Building with --checkout and no path prints the output's path in the store.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.file("Hello, World!");
	'
}

let output = tg build --checkout $path | complete
success $output

let checkout = $output.stdout | from json
assert equal ($checkout | path dirname) ($local.directory | path join 'store')
assert equal (open --raw $checkout) 'Hello, World!'
