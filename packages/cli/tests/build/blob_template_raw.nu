use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => tg.Blob.raw`\n\tHello, World!\n`.then((b) => b.text());
	'
}

let output = tg build $path | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
