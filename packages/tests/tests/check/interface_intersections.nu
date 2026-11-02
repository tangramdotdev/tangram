use ../../util/test.nu *

# A Python caller can use an intersection of TypeScript records, including a named type exported by another module.

let local = server spawn
let modules = {
	args.tg.ts: '
		export type Arg = { name: string; punctuation?: string } & { name: "world" | "moon" };
	'
	hello.tg.ts: '
		import type { Arg } from "./args.tg.ts";
		export function greet(arg: Arg): string {
			return `Hello, ${arg.name}${arg.punctuation ?? "!"}`;
		}
	'
}
let caller = '
	# /// script
	# [tool.tangram.dependencies.hello]
	# specifier = "./hello.tg.ts"
	# ///
	from hello import greet
	async def default() -> str:
		return await greet.build(ARG)
'
let path = artifact ($modules | merge {
	tangram.py: ($caller | str replace ARG '{"name": "world"}')
})
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, world!'

# The interface preserves the merged field type and the optional field.
let output = tg interface --language json ($path | path join args.tg.ts) | complete
success $output
let fields = $output.stdout | from json | get definitions.Arg.fields
assert equal $fields.name.type.kind union
assert equal ($fields.name.type.types | get value | sort) [moon world]
assert equal $fields.punctuation { optional: true, type: { kind: string } }

# Invalid arguments fail checking against the record rather than an unsupported export.
for arg in ['{}' '42' '{"name": "world", "punctuation": 42}'] {
	let path = artifact ($modules | merge {
		tangram.py: ($caller | str replace ARG $arg)
	})
	let output = tg check $path | complete
	failure $output
	assert ($output.stderr | str contains 'tangram.py:') $output.stderr
	assert (not ($output.stderr | str contains 'NonValue')) $output.stderr
}

# An intersection that brands a primitive does not become a record.
let path = artifact {
	tangram.ts: '
		export function branded(value: string & { readonly brand: "name" }) { return value; }
	'
}
let output = tg interface --language json $path | complete
success $output
assert ('error' in ($output.stdout | from json | get exports.branded))
