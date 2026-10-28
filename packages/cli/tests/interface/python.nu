use ../lib/test.nu *

# The interface of a Python module lists its public exports with their docstrings and the ranges of their names, and types each export as a command.

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
			range: { start: { line: 4, character: 0 }, end: { line: 4, character: 6 } },
			type: $command,
		},
		greet: {
			documentation: 'Greet someone.',
			range: { start: { line: 0, character: 4 }, end: { line: 0, character: 9 } },
			type: $command,
		},
	},
}
assert equal $interface $expected
