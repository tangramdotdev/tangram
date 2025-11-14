use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': 'export default () => new Uint8Array([1,2,3]);'
}

let output = tg build $path | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
