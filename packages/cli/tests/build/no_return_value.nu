use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': 'export default () => {};'
}

let output = tg build $path | complete
assert equal $output.exit_code 0
assert (snapshot ($output.stdout | str trim))
