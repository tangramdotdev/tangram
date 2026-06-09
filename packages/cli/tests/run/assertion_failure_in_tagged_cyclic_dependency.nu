use ../../test.nu *

# A failing tg.assert reached through a tagged, cyclic dependency causes the run to fail and produces the expected diagnostic on stderr.

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

let output = do { cd $path; tg run } | complete
failure $output
let stderr = $output.stderr
let stderr = $stderr | redact | normalize_ids
snapshot $stderr
