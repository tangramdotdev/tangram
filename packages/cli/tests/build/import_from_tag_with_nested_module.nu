use ../../test.nu *

let server = spawn

# Create and tag package a.
let a_path = artifact {
	tangram.ts: '
		export { foo } from "./foo/foo.tg.ts";
		export { bar } from "./bar.tg.ts";
		export const baz = () => "baz";
	'
	bar.tg.ts: '
		import * as b from "./tangram.ts";
		export const bar = () => b.foo();
	'
	foo: {
		foo.tg.ts: '
			export const foo = () => "foo";
		'
	}
}
run tg tag a $a_path

# Create package b that imports a.
let b_path = artifact {
	tangram.ts: '
		import * as a from "a";
		export default async () => {
		  return a.bar();
		};
	'
}

run tg build $b_path

# Mutate b to call a.baz() instead.
let new_tangram = '
	import * as a from "a";
	export default async () => {
	  return a.baz();
	};
'
$new_tangram | save -f ($b_path | path join 'tangram.ts')

# Rebuild.
run tg build $b_path
