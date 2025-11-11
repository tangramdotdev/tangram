use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': "
		export default () => {
			console.log(\"Hello, World!\");
		};
	"
}

let output = tg run $path | complete
assert equal $output.exit_code 0
assert (snapshot ($output.stdout | str trim))
