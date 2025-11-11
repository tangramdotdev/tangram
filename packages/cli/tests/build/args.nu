use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': 'export default (name: string) => `Hello, ${name}!`;'
}

let output = tg build $path -a 'Tangram' | complete
assert equal $output.exit_code 0
assert (snapshot ($output.stdout | str trim))
