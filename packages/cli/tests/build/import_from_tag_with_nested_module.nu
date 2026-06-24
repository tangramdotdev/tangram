use ../../test.nu *

# A package importing a tagged dependency can resolve exports re-exported from nested modules, both on the initial build and after the importing package is mutated to call a different export.

let server = spawn

# Create and tag package a.
let a_path = artifact {
	tangram.ts: '
		export { foo } from "./foo/foo.tg.ts";
		export { bar } from "./bar.tg.ts";
		export function baz() { return "baz"; }
	'
	bar.tg.ts: '
		import * as b from "./tangram.ts";
		export function bar() { return b.foo(); }
	'
	foo: {
		foo.tg.ts: '
			export function foo() { return "foo"; }
		'
	}
}
tg tag a $a_path

# Create package b that imports a.
let b_path = artifact {
	tangram.ts: '
		import * as a from "a";
		export default async function () {
			return a.bar();
		}
	'
}

tg build $b_path

# Mutate b to call a.baz() instead.
let new_tangram = '
	import * as a from "a";
	export default async function () {
		return a.baz();
	}
'
$new_tangram | save --force ($b_path | path join 'tangram.ts')

# Rebuild.
tg build $b_path
