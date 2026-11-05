use ../../util/test.nu *

let local = server spawn

# Recursive aliases retain their structure across command and module boundaries, including when a nullable or optional member flattens a union alias.
let modules = {
	types.tg.ts: '
		export type Condition = { all: Array<Condition> } | { value: string };
		export type NestedArray = Array<string | NestedArray>;
		export type Deferred = string | Array<Promise<Deferred>>;
		export type Nullable = { next: Nullable | null } | { value: string };
		export type Optional = { next?: Optional } | { value: string };
	'
	values.tg.ts: '
		import type { Condition, Deferred, NestedArray, Nullable, Optional } from "./types.tg.ts";
		type Box<T> = T | Array<Box<T>>;
		type NestedTuple = [string, NestedTuple | null];
		type Tree = { children: Array<Tree> } | string;
		type Outer = { inner: Inner } | string;
		type Inner = { outer: Outer };
		export function box(strings: Box<string>, numbers: Box<number>): void {}
		export function condition(value: Condition): Condition { return value; }
		export function array(value: NestedArray): NestedArray { return value; }
		export function nestedTuple(value: NestedTuple): NestedTuple { return value; }
		export function deferred(): Deferred { return [Promise.resolve("ready")]; }
		export function nullable(value: Nullable | null): Nullable | null { return value; }
		export function optional(value: Optional): Optional { return value; }
		export function outer(value: Outer): void {}
		export function trees(value: Array<Tree>): void {}
	'
}
let imports = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# [tool.tangram.dependencies.types]
	# specifier = "./types.tg.ts"
	# ///
	from types import Nullable, Optional
	from values import array, box, condition, deferred, nestedTuple, nullable, optional, outer, trees
'
let path = artifact ($modules | merge { tangram.py: ($imports + '
	async def default():
	    await condition.build({"all": [{"value": "leaf"}]})
	    await array.build(["root", ["leaf"]])
	    await box.build(["root", ["leaf"]], [1.0, [2.0]])
	    await nestedTuple.build(("root", ("leaf", None)))
	    await deferred.build()
	    nullable_value: Nullable = {"next": {"next": None}}
	    await nullable.build(nullable_value)
	    await nullable.build(None)
	    optional_value: Optional = {"next": {"value": "leaf"}}
	    await optional.build(optional_value)
	    await outer.build({"inner": {"outer": "leaf"}})
	    await trees.build(["leaf", {"children": ["leaf"]}])
') })
success (tg check $path | complete)

# An exported recursive alias is defined by the module that exports it, which importers refer to.
let output = tg interface --language json ($path | path join types.tg.ts) | complete
success $output
let interface = $output.stdout | from json
assert equal $interface.definitions.NestedArray { kind: array, item: { kind: union, types: [{ kind: string }, { kind: reference, name: NestedArray }] } }
let output = tg interface --language json ($path | path join values.tg.ts) | complete
success $output
let interface = $output.stdout | from json
assert equal $interface.exports.array.type.output { kind: reference, import: types, name: NestedArray }
assert ($interface.exports.trees.type.args.elements.0.type.name | str starts-with _tg_recursive)

# Invalid nested values and incompatible output types are still rejected.
for statement in [
	'await condition.build({"all": [{"value": 42}]})'
	'await array.build([[42]])'
	'await box.build([42.0], [1.0])'
	'await box.build(["root"], ["leaf"])'
	'await nestedTuple.build(("root", (42, None)))'
	'value: float = await deferred.build()'
	'await nullable.build({"next": {"next": 42}})'
	'value: Nullable = {"next": {"next": 42}}'
	'await optional.build({"next": {"value": 42}})'
	'value: Optional = {"next": {"value": 42}}'
	'await outer.build({"inner": {"outer": 42}})'
	'await trees.build([42])'
] {
	let source = (doc $imports) + "\nasync def default():\n    " + $statement + "\n"
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:10:') $output.stderr
}

# An alias that does not refer to itself is converted in place, with no definition.
let path = artifact {
	tangram.ts: '
		export type Color = "red" | "green";
		export type Foo = { a: string };
		export type Items = Array<Foo>;
		type Maybe<T> = T | null;
		type Hidden = { b: string };
		export type Unused = Array<Hidden>;
		export function f(color: Color, items: Items, maybe: Maybe<string>, promise: tg.MaybePromise<string>): void {}
	'
}
let output = tg interface --language json ($path | path join tangram.ts) | complete
success $output
let interface = $output.stdout | from json
assert equal ($interface.definitions | columns) [Foo]
assert equal ($interface.exports.f.type.args.elements | get type.kind) [union array union string]

# A definition of another type does not take the name of an exported type, whether or not the other type refers to itself.
for kind in ['{ y: string }' 'string | Array<Kind>'] {
	let path = artifact {
		tangram.ts: ('
			export namespace ns { export type Kind = ' + $kind + '; }
			export type Aaa = { other: ns.Kind; exported: Kind };
			type Kind = { x: string };
			export { Kind };
		')
	}
	let output = tg interface --language json ($path | path join tangram.ts) | complete
	success $output
	let interface = $output.stdout | from json
	assert equal $interface.definitions.Kind { kind: record, fields: { x: { type: { kind: string } } } }
	assert equal $interface.definitions.Aaa.fields.exported.type { kind: reference, name: Kind }
	assert equal $interface.definitions.Aaa.fields.other.type { kind: reference, name: Kind2 }
}

# The args of a command keep their types when a recursive alias names them.
let modules = {
	shapes.tg.ts: '
		export type Imported = [string, Imported | null];
		export type ImportedArray = Array<string | ImportedArray>;
	'
	values.tg.ts: '
		import type { Imported, ImportedArray } from "./shapes.tg.ts";
		export type Rec = [string, Rec | null];
		export type NestedArgs = Array<string | NestedArgs>;
		export function imported(c: tg.Command<Imported, string>): string { return "x"; }
		export function importedArray(c: tg.Command<ImportedArray, string>): string { return "x"; }
		export function recursive(c: tg.Command<Rec, string>): string { return "x"; }
		export function recursiveArray(c: tg.Command<NestedArgs, string>): string { return "x"; }
	'
}
let imports = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# [tool.tangram.dependencies.shapes]
	# specifier = "./shapes.tg.ts"
	# ///
	from shapes import Imported, ImportedArray
	from values import NestedArgs, Rec, imported, importedArray, recursive, recursiveArray
'
let path = artifact ($modules | merge { tangram.py: ($imports + '
	async def default(
	    other: tg.Command[Imported, str],
	    other_array: tg.Command[tuple[str | ImportedArray, ...], str],
	    rec: tg.Command[tuple[str, Rec | None], str],
	    nested: tg.Command[tuple[str | NestedArgs, ...], str],
	):
	    await imported.build(other)
	    await importedArray.build(other_array)
	    await recursive.build(rec)
	    await recursiveArray.build(nested)
') })
success (tg check $path | complete)
for statement in [
	'await imported.build(wrong)'
	'await importedArray.build(wrong)'
	'await recursive.build(wrong)'
	'await recursiveArray.build(wrong)'
] {
	let source = (doc $imports) + "\nasync def default(wrong: tg.Command[tuple[bool], str]):\n    " + $statement + "\n"
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:10:') $output.stderr
}

# A Python alias converts to a definition only if it refers to itself, including an alias that a TypeScript module exports.
let modules = {
	shapes.tg.ts: '
		export type NestedArray = Array<string | NestedArray>;
		export type Nullable = { next: Nullable | null } | { value: string };
	'
	values.tg.py: '
		# /// script
		# [tool.tangram.dependencies.shapes]
		# specifier = "./shapes.tg.ts"
		# ///
		from shapes import NestedArray, Nullable
		from typing import TypedDict
		type Flat = str | None
		type Local = list[str | Local]
		type Later = list[str | Later]
		type Box[T] = T | list[Box[T]]
		class FailingDict(TypedDict, closed=True):
		    alias: Later
		    other: set[str]
		def box(strings: Box[str], numbers: Box[float]) -> None:
		    return None
		def command(c: tg.Command[Local, str]) -> str:
		    return "x"
		def failing(value: Later, other: set[str]) -> None:
		    return None
		def flat(value: Flat) -> Flat:
		    return value
		def from_ts(value: NestedArray) -> NestedArray:
		    return value
		def later(value: Later) -> Later:
		    return value
		def local(value: Local) -> Local:
		    return value
		def nullable(value: Nullable) -> Nullable:
		    return value
	'
}
let path = artifact ($modules | merge { tangram.ts: '
	import { box, command, from_ts, later, local, nullable, type Local } from "./values.tg.py";
	export default async (args: tg.Command<Array<string | Local>, string>) => {
		await box.build(["root", ["leaf"]], [1, [2]]);
		await command.build(args);
		await from_ts.build(["root", ["leaf"]]);
		await later.build(["root", ["leaf"]]);
		await local.build(["root", ["leaf"]]);
		await nullable.build({ next: { value: "leaf" } });
	};
' })
success (tg check $path | complete)
let output = tg interface --language json ($path | path join values.tg.py) | complete
success $output
let interface = $output.stdout | from json
assert equal $interface.exports.flat.type.output { kind: union, types: [{ kind: string }, { kind: 'null' }] }
assert equal $interface.exports.from_ts.type.output { kind: reference, name: NestedArray }
assert equal $interface.exports.local.type.output { kind: reference, name: Local }
assert equal $interface.definitions.Local { kind: array, item: { kind: union, types: [{ kind: string }, { kind: reference, name: Local }] } }
assert equal ($interface.definitions | columns | where $it !~ '^_tg' | sort) [Box Box2 Later Local NestedArray]
for statement in [
	'await box.build([42], [1]);'
	'await box.build(["root"], ["leaf"]);'
	'await command.build(wrong);'
	'await from_ts.build([42]);'
	'await later.build([42]);'
	'await local.build([42]);'
	'await nullable.build({ next: { value: 42 } });'
] {
	let source = '
		import { box, command, from_ts, later, local, nullable } from "./values.tg.py";
		export default async (wrong: tg.Command<[boolean], string>) => {
			' + $statement + '
		};
	'
	let path = artifact ($modules | merge { tangram.ts: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.ts:3:') $output.stderr
}
