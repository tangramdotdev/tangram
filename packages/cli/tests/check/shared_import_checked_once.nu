use ../lib/test.nu *

# A module imported along two paths is checked once, so its type error is reported once.

let server = server spawn

let path = artifact {
	a.tg.ts: '
		import * as lib from "lib" with { source: "./lib.tg.ts" };
		export default function () {}
	'
	lib.tg.ts: '
		export let value: number = "value";
	'
	tangram.ts: '
		import * as a from "a" with { source: "./a.tg.ts" };
		import * as lib from "lib" with { source: "./lib.tg.ts" };
		export default function () {}
	'
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
