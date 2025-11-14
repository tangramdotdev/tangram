use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export let five = () => 5;
		export let six = () => 6;
	'
}

let output = tg build ($path + '#five') | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
