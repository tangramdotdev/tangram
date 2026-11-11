use ../../util/test.nu *

let local = server spawn
let modules = {
	shapes.tg.ts: '
		export type Arg = { text?: string; child?: Child; children?: Array<Child>; entries?: { [key: string]: Child } };
		export type Child = { count: number };
		export type WideChild = { count: string | number };
		export type Input = { text?: string | tg.Mutation<string>; child?: WideChild; children?: Array<Child>; entries?: { [key: string]: Child } };
		export type Node = { text: string; next?: Node };
		export type inputs = { text: string };
	'
	native.tg.py: '
		from typing import TypedDict
		__all__ = ["Native"]
		class Native(TypedDict, closed=True):
		    text: str
	'
	values.tg.ts: '
		import type { Arg, Input, Node, inputs } from "./shapes.tg.ts";
		import type { Native } from "./native.tg.py";
		export function echo(...args: Array<Input>): Arg { return { text: "value" }; }
		export function node(arg: Node): Node { return arg; }
		export function native(arg: Native): string { return arg.text; }
		export function accepts(command: tg.Command<Array<Arg>, Arg>): string { return "command"; }
		export function named(arg: inputs): inputs { return arg; }
		type Tree = string | Array<Tree>;
		export function tree(arg: Tree): string { return "tree"; }
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
	    await values.named.build({"text": "named"})
	    await values.tree.build(["leaf", ["branch"]])
	    return output["text"]
'
let path = artifact $modules
($header + "\n" + $caller) | save ($path | path join tangram.py)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'returned'

# Invalid fields and incompatible command handles remain errors, which name the types as the source does.
for case in [
	{ body: 'await values.echo.build({"text": 42})', error: 'key "text" with declared type `str | Mutation[Unknown]` on TypedDict `Input`' }
	{ body: 'await values.echo.build({"child": {"count": None}})', error: 'key "count" with declared type `str | float` on TypedDict `WideChild`' }
	{ body: 'arg: shapes.Arg = {"text": 42}', error: 'key "text" with declared type `str` on TypedDict `Arg`' }
	{ body: 'output = await values.echo.build(); output["text"] = 42', error: 'key "text" with declared type `str` on TypedDict `Arg`' }
	{ body: 'await values.accepts.build(command)', error: 'Expected `Command[tuple[Arg, ...], Arg]`, found `Command[tuple[float], Arg]`' }
	{ body: 'output = await values.named.build({"text": "value"}); output["text"] = 42', error: 'key "text" with declared type `str` on TypedDict `inputs`' }
	{ body: 'await values.tree.build(42)', error: 'Expected `Tree`, found `Literal[42]`' }
] {
	let path = artifact $modules
	($header + "\nasync def default(command: tg.Command[tuple[float], shapes.Arg]):\n    " + $case.body + "\n") | save ($path | path join tangram.py)
	let output = tg check $path | complete
	failure $output $case.body
	assert ($output.stderr | str contains $case.error) $output.stderr
}
