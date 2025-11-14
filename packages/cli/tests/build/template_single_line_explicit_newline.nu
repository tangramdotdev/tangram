use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		import foo from "./foo.txt";
		import bar from "./bar.txt";
		export default () => tg`${foo}\n${bar}`;
	'
	'foo.txt': 'foo'
	'bar.txt': 'bar'
}

let output = tg build $path | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
