use std assert
use ../../test.nu *

let server = spawn

# Create and tag the foo dependency.
let foo_path = artifact {
	tangram.ts: '
		export default () => tg.assert(false, "error in foo");
	'
}
tg tag foo $foo_path

let path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default () => foo();
	'
}

let output = tg run $path | complete
failure $output
snapshot $output.stdout
