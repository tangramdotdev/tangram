use ../../util/test.nu *

let local = server spawn
let path = artifact {
	shapes.tg.ts: '
		export type Arg = { text?: string; child?: Child; children?: Array<Child>; entries?: { [key: string]: Child } };
		export type Child = { count: number };
		export type WideChild = { count: string | number };
		export type Input = { text?: string | tg.Mutation<string>; child?: WideChild; children?: Array<Child>; entries?: { [key: string]: Child } };
		export type Node = { text: string; next?: Node };
	'
	native.tg.py: '
		from typing import TypedDict
		__all__ = ["Native"]
		class Native(TypedDict, closed=True):
		    text: str
	'
	values.tg.ts: '
		import type { Arg, Input, Node } from "./shapes.tg.ts";
		import type { Native } from "./native.tg.py";
		export function echo(...args: Array<Input>): Arg { return { text: "value" }; }
		export function node(arg: Node): Node { return arg; }
		export function native(arg: Native): string { return arg.text; }
		export function accepts(command: tg.Command<Array<Arg>, Arg>): string { return "command"; }
	'
}
let header = doc '
	# /// script
	# [tool.tangram.dependencies.shapes]
	# specifier = "./shapes.tg.ts"
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from __future__ import annotations
	import shapes
	import values
'

# Mutable records can be forwarded to wider inputs without changing their named or returned types.
let caller = doc '
	async def default(*args: shapes.Arg) -> str:
	    arg: shapes.Arg = {"text": "value", "child": {"count": 1}, "children": [{"count": 2}], "entries": {"one": {"count": 3}}}
	    arg["text"] = "edited"
	    output = await values.echo.build(arg, *args)
	    output["text"] = "returned"
	    node: shapes.Node = {"text": "node", "next": {"text": "leaf"}}
	    await values.node.build(node)
	    await values.native.build({"text": "native"})
	    await values.accepts.build(values.echo)
	    return output["text"]
'
($header + "\n" + $caller) | save ($path | path join tangram.py)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'returned'

# Invalid fields and incompatible command handles remain errors.
for body in [
	'await values.echo.build({"text": 42})'
	'await values.echo.build({"child": {"count": None}})'
	'arg: shapes.Arg = {"text": 42}'
	'output = await values.echo.build(); output["text"] = 42'
	'await values.accepts.build(command)'
] {
	($header + "\nasync def default(command: tg.Command[tuple[float], shapes.Arg]):\n    " + $body + "\n") | save --force ($path | path join tangram.py)
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:') $output.stderr
}
