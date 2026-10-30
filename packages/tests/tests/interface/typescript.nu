use ../../util/test.nu *

# The interface of a TypeScript module lists its exports with their documentation and the ranges of their names, and types each export as a command with the types that the checker infers.

let local = server spawn

let path = artifact {
	tangram.ts: '
		/** Greet someone. */
		export function greet(name: string) { return `Hello, ${name}!`; }
		export const answer = 42;
	'
}

let output = tg interface --language json $path | complete
success $output
let interface = $output.stdout | from json

let expected = {
	exports: {
		answer: {
			range: { start: { line: 2, character: 13 }, end: { line: 2, character: 19 } },
			type: {
				kind: command,
				args: { kind: tuple, elements: [] },
				output: { kind: literal, value: 42 },
			},
		},
		greet: {
			documentation: 'Greet someone.',
			range: { start: { line: 1, character: 16 }, end: { line: 1, character: 21 } },
			type: {
				kind: command,
				args: { kind: tuple, elements: [{ type: { kind: string } }] },
				output: { kind: string },
			},
		},
	},
}
assert equal $interface $expected

# An export whose type is not a value has the reason instead of a type.
let path = artifact {
	tangram.ts: '
		export function curry(name: string) { return () => name; }
	'
}
let output = tg interface --language json $path | complete
success $output
let curry = $output.stdout | from json | get exports.curry
assert ('type' not-in $curry)
assert ($curry.error | str contains 'a function is not a value') $curry.error
