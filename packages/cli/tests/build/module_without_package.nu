use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'foo.tg.ts': '
		export default () => "Hello, World!";
	'
}

let output = tg build ($path | path join './foo.tg.ts') | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
