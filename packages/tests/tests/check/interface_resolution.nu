use ../../util/test.nu *

# Inputs accept values from structural types, while outputs resolve nested promises.
let local = server spawn
let modules = {
	types.tg.ts: '
		export type Local = tg.Unresolved<{ text: string }>;
		export interface Choice { value: string | PromiseLike<number>; }
		export type Box<T> = { value: T };
	'
	values.tg.ts: '
		import type { Box, Choice, Local } from "./types.tg.ts";
		export async function alias(value: Local): Promise<string> {
			return (await tg.resolve(value)).text;
		}
		export function choice(value: Choice): Choice {
			return { value: Promise.resolve(42) };
		}
		export function boxes(value: Box<string>): Box<number> {
			return { value: value.value.length };
		}
		type Nested = { nested: Array<Promise<string>> };
		export const constant: Nested = { nested: [Promise.resolve("ready")] };
		export function result() { return constant; }
		export function required(value: PromiseLike<string>): PromiseLike<string> {
			return value.then(text => text);
		}
		export function unsupported(value: string | Date): string { return "unused"; }
		export async function execute(value: (() => string) | tg.Command<Array<tg.Value>, string>): Promise<string> {
			return typeof value === "function" ? value() : await value.build();
		}
	'
}
let imports = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import alias, boxes, choice, constant, execute, required, result, unsupported
'
let caller = $imports + '
	def message() -> str:
	    return "command"
	async def default() -> list[str | float]:
	    text: str = await alias.build({"text": "alias"})
	    selected: str | float = (await choice.build({"value": "input"}))["value"]
	    length: float = (await boxes.build({"value": "four"}))["value"]
	    exported: str = (await constant.build())["nested"][0]
	    returned: str = (await result.build())["nested"][0]
	    command: str = await execute.build(await tg.command(message))
	    return [text, selected, length, exported, returned, command]
'
let path = artifact ($modules | merge { tangram.py: $caller })
success (tg check ($path | path join values.tg.ts) | complete)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) [alias 42 4 ready ready command]

# An input promise is never replaced by its result, and unsupported branches remain errors.
for statement in [
	'await choice.build({"value": 42})'
	'await boxes.build({"value": 42})'
	'await required.build("plain")'
	'await unsupported.build("plain")'
	'await execute.build("plain")'
	'value: str = (await choice.build({"value": "input"}))["value"]'
	'value: str = (await boxes.build({"value": "four"}))["value"]'
	'value: int = (await constant.build())["nested"][0]'
	'value: int = (await result.build())["nested"][0]'
] {
	let source = (doc $imports) + "\nasync def default():\n    " + $statement + "\n"
	let path = artifact ($modules | merge { tangram.py: $source })
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
}
