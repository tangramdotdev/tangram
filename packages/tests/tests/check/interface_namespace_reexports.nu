use ../../util/test.nu *

let local = server spawn
let path = artifact {
	types.tg.ts: '
		export * as tools from "./tools.tg.ts";
		export type Arg = { text: string };
	'
	tools.tg.ts: '
		export function tool(): string { return "tool"; }
	'
	values.tg.ts: '
		import type { Arg } from "./types.tg.ts";
		export function echo(value: Arg): Arg { return value; }
	'
}
let header = doc '
	# /// script
	# [tool.tangram.dependencies.types]
	# specifier = "./types.tg.ts"
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from __future__ import annotations
	import types
	from values import echo
	async def default() -> str:
'

# A namespace re-export does not block named types or commands that refer to them.
($header + "\n    arg: types.Arg = {\"text\": \"value\"}\n    return (await echo.build(arg))[\"text\"]\n") | save ($path | path join tangram.py)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'value'

# The imported type and the namespace retain their constraints.
for case in [
	{ body: 'return (await echo.build({"text": 42}))["text"]', error: 'Argument to bound method' }
	{ body: 'return await types.tools.build()', error: 'a function is not a value' }
] {
	($header + "\n    " + $case.body + "\n") | save --force ($path | path join tangram.py)
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:') $output.stderr
	assert ($output.stderr | str contains $case.error) $output.stderr
}
