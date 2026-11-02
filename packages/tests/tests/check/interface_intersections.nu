use ../../util/test.nu *

# A Python caller can use an intersection of TypeScript records, including a named type exported by another module.

let local = server spawn
let path = artifact {
	args.tg.ts: '
		export type Arg = { name: string; punctuation?: string } & { name: "world" };
	'
	hello.tg.ts: '
		import type { Arg } from "./args.tg.ts";
		export function greet(arg: Arg): string {
			return `Hello, ${arg.name}${arg.punctuation ?? "!"}`;
		}
	'
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.hello]
		# specifier = "./hello.tg.ts"
		# ///
		from hello import greet
		async def default() -> str:
			return await greet.build({"name": "world"})
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, world!'

# The interface preserves the narrowed field type and the optional field.
let output = tg interface --language json ($path | path join args.tg.ts) | complete
success $output
assert equal ($output.stdout | from json | get definitions.Arg.fields) {
	name: { type: { kind: literal, value: world } }
	punctuation: { optional: true, type: { kind: string } }
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
