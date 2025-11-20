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

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $stderr
