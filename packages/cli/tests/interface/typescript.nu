use ../lib/test.nu *

# The interface of a TypeScript module lists its exports with their documentation and the ranges of their names, and types each export as a command.

let local = server spawn

let path = artifact {
	tangram.ts: '
		/** Greet someone. */
		export function greet(name: string) { return `Hello, ${name}!`; }
		export const answer = 42;
	'
}

let output = tg interface $path | complete
success $output
let interface = $output.stdout | from json

let command = {
	kind: command,
	args: { kind: array, item: { kind: value } },
	output: { kind: value },
}
let expected = {
	exports: {
		answer: {
			range: { start: { line: 2, character: 13 }, end: { line: 2, character: 19 } },
			type: $command,
		},
		greet: {
			documentation: 'Greet someone.',
			range: { start: { line: 1, character: 16 }, end: { line: 1, character: 21 } },
			type: $command,
		},
	},
}
assert equal $interface $expected
