use ../lib/test.nu *

# A module imported along two paths is checked once, regardless of the import order.

let server = server spawn

for imports in [
	'
		import "a" with { source: "./a.tg.ts" };
		import "lib" with { source: "./lib.tg.ts" };
	'
	'
		import "lib" with { source: "./lib.tg.ts" };
		import "a" with { source: "./a.tg.ts" };
	'
] {
	let path = artifact {
		a.tg.ts: '
			import "lib" with { source: "./lib.tg.ts" };
		'
		lib.tg.ts: '
			export let value: number = "value";
		'
		tangram.ts: $imports
	}

	let output = tg check $path | complete
	failure $output
	snapshot --normalize --redact $path $output.stderr r#'
		error Type 'string' is not assignable to type 'number'.
		   ╭─[<redacted>/lib.tg.ts:1:12]
		 1 │ export let value: number = "value";
		   ·            ──┬──
		   ·              ╰── Type 'string' is not assignable to type 'number'.
		   ╰────
		error an error occurred
		-> type checking failed

	'#
}
