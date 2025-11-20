use std assert
use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		import foo from "./foo.tg.ts";
		export default () => foo();
	',
	foo.tg.ts: '
		export default () => tg.assert(false);
	',
}

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr | lines | skip 1 | str join "\n"
let stderr = $stderr | str replace -ar 'pcs_00[0-9a-z]{26}' 'PROCESS'
snapshot $stderr
