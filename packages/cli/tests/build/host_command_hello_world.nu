use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default async () => {
			return await tg.run`echo "Hello, World!" > $OUTPUT`;
		};
	'
}

let output = tg build $path | complete
assert (success $output)
assert (snapshot $output.stdout)
