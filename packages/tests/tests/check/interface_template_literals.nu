use ../../util/test.nu *

# Open-ended template literal types, including checksums, cross the language boundary as strings.

let local = server spawn
let modules = {
	values.tg.ts: '
		export type Arg = { checksum: tg.Checksum; label: `item-${number}` };
		export function record(arg: Arg): Arg { return arg; }
		export function checksum(value: tg.Checksum): tg.Checksum { return value; }
		export function finite(value: `mode-${"a" | "b"}`) { return value; }
	'
}
let caller = '
	# /// script
	# [tool.tangram.dependencies.values]
	# specifier = "./values.tg.ts"
	# ///
	from values import checksum, record
	async def default() -> list[str]:
	    value: str = await checksum.build("sha256:abc")
	    result = await record.build({"checksum": "sha512-any", "label": "item-1"})
	    digest: str = result["checksum"]
	    label: str = result["label"]
	    return [value, digest, label]
'
let path = artifact ($modules | merge { tangram.py: $caller })
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) ['sha256:abc' 'sha512-any' 'item-1']

# Finite template literal types retain their exact alternatives.
let output = tg interface --language json ($path | path join values.tg.ts) | complete
success $output
let finite = $output.stdout | from json | get exports.finite.type
let expected = { kind: union, types: [{ kind: literal, value: mode-a } { kind: literal, value: mode-b }] }
assert equal $finite.args.elements.0.type $expected
assert equal $finite.output $expected

# Scalar parameters and record fields still reject non-string arguments.
let source = $caller
	| str replace '"sha256:abc"' '42'
	| str replace '"item-1"' '42'
let path = artifact ($modules | merge { tangram.py: $source })
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr
assert ($output.stderr | str contains 'tangram.py:8:') $output.stderr

# Scalar and record outputs retain their string types.
let source = $caller | str replace -a ': str =' ': int ='
let path = artifact ($modules | merge { tangram.py: $source })
let output = tg check $path | complete
failure $output
for line in [7 9 10] {
	assert ($output.stderr | str contains $'tangram.py:($line):') $output.stderr
}
