use std assert
use ../../test.nu *

let server = spawn

let foo = artifact {
	foo: {
		tangram.ts: '
			import bar from "../bar";
			export default () => bar();
			export const failure = () => tg.assert(false, "failure in foo");
		'
	}
	bar: {
		tangram.ts: '
			import { failure } from "../foo";
			export default () => failure();
		'
	}
}
tg tag foo ($foo | path join 'foo')

let path = artifact {
	tangram.ts: '
		import foo from "foo";
		export default () => foo();
	'
}

let output = tg run $path | complete
failure $output
snapshot $output.stderr
