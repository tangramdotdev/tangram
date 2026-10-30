use ../../util/test.nu *

# The interface of a Python module lists its public exports with their docstrings and the ranges of their names, and types each export as a command with the types that the checker infers.

let local = server spawn

let path = artifact {
	tangram.py: '
		def greet(name):
		    """Greet someone."""
		    return f"Hello, {name}!"

		answer = 42
		_hidden = 1
	'
}

let output = tg interface --syntax json $path | complete
success $output
let interface = $output.stdout | from json

let expected = {
	exports: {
		answer: {
			range: { start: { line: 4, character: 0 }, end: { line: 4, character: 6 } },
			type: {
				kind: command,
				args: { kind: tuple, elements: [] },
				output: { kind: literal, value: 42 },
			},
		},
		greet: {
			documentation: 'Greet someone.',
			range: { start: { line: 0, character: 4 }, end: { line: 0, character: 9 } },
			type: {
				kind: command,
				args: { kind: tuple, elements: [{ type: { kind: value } }] },
				output: { kind: value },
			},
		},
	},
}
assert equal $interface $expected

# An export whose type is not a value has the reason instead of a type.
let path = artifact {
	tangram.py: '
		from collections.abc import Callable
		__all__ = ["curry"]
		def curry(name: str) -> Callable[[], str]:
		    return lambda: name
	'
}
let output = tg interface --syntax json $path | complete
success $output
let curry = $output.stdout | from json | get exports.curry
assert ('type' not-in $curry)
assert ($curry.error | str contains 'a function is not a value') $curry.error
