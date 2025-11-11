use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': "
		export default async () => {
			return await tg.run(\"echo 'Hello, World!' > $OUTPUT\");
		};
	"
}

let output = tg build $path | complete
print $output
assert equal $output.exit_code 0
assert (snapshot ($output.stdout | str trim))
