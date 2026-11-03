use ../../util/test.nu *

# An interface refers to imported types without its cost or validity depending on the shape of the imported type graph.

let local = server spawn

def caller [name: string, body: string] {
	let header = '
		# /// script
		# [tool.tangram.dependencies.values]
		# specifier = "./values.tg.ts"
		# ///
	'
	(doc $header) + "\nfrom values import " + $name + "\nasync def default() -> str:\n    " + $body + "\n"
}

# An imported type that reaches each type through several paths checks quickly.
let depth = 24
let types = 0..<$depth
	| each { |index| "export type T" + ($index | into string) + " = { a: T" + ($index + 1 | into string) + "; b: T" + ($index + 1 | into string) + "; c: T" + ([($index + 2) $depth] | math min | into string) + " };" }
	| append ("export type T" + ($depth | into string) + " = { text: string };")
	| str join "\n"
let path = artifact {
	types.tg.ts: $types
	values.tg.ts: '
		import type { T0 } from "./types.tg.ts";
		export function echo(value: T0): T0 { return value; }
	'
	tangram.py: (caller echo 'unused = echo.build; return "unused"')
}
success (tg check $path | complete)

# An imported type that is nested more deeply than the conversion limit remains usable.
let depth = 40
let types = 0..<$depth
	| each { |index| "export type T" + ($index | into string) + " = { next: T" + ($index + 1 | into string) + " };" }
	| append ("export type T" + ($depth | into string) + " = { text: string };")
	| str join "\n"
let access = 0..<$depth | each { '["next"]' } | str join ''
let path = artifact {
	types.tg.ts: $types
	values.tg.ts: '
		import type { T0 } from "./types.tg.ts";
		export function make(): T0 {
			let value: any = { text: "deep" };
			for (let index = 0; index < 40; index++) {
				value = { next: value };
			}
			return value;
		}
	'
	tangram.py: (caller make ('return (await make.build())' + $access + '["text"]'))
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'deep'

# A type that fails in an imported cycle fails from every entry, and does not affect the other exports.
let modules = {
	types.tg.ts: '
		export type A = { b: B; bad: () => string };
		export type B = { a: A };
		export type Good = { text: string };
	'
	values.tg.ts: '
		import type { A, B, Good } from "./types.tg.ts";
		export function first(value: A): string { return "unused"; }
		export function second(value: B): string { return "unused"; }
		export function third(value: B): string { return "unused"; }
		export function good(value: Good): string { return value.text; }
	'
}
let path = artifact ($modules | merge { tangram.py: (caller good 'return await good.build({"text": "good"})') })
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'good'
for name in [first second third] {
	let path = artifact ($modules | merge { tangram.py: (caller $name ('return await ' + $name + '.build({})')) })
	failure (tg check $path | complete)
}

# A type that is not a shared value in an imported cycle is still resolved at the output boundary.
let path = artifact {
	types.tg.ts: '
		export type P = { later: Promise<string>; q?: Q };
		export type Q = { p: P };
	'
	values.tg.ts: '
		import type { P } from "./types.tg.ts";
		export function output(): P { return { later: Promise.resolve("ready") }; }
	'
	tangram.py: (caller output 'return (await output.build())["later"]')
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'ready'
