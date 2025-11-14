use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => {
			let a = "string!";
			return tg.file`\n\tHello, World!\n\t${a}\n`.then((f) => f.text());
		}
	'
}

let output = tg build $path | complete
assert equal $output.exit_code 0
assert (snapshot $output.stdout)
