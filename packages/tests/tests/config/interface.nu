use ../../util/test.nu *

# A config module's interface comes from its runtime, or from an interface beside the config, and importers in TypeScript and Python check against it.

let local = server spawn

let runtime = '
	export default async (arg: { export: string; args: Array<tg.Value> }) => {
		if (arg.export === "interface") {
			return {
				exports: {
					greet: {
						type: {
							kind: "command",
							args: { kind: "tuple", elements: [{ type: { kind: "string" } }] },
							output: { kind: "string" },
						},
					},
				},
			};
		}
		tg.assert(arg.export === "greet");
		return `Hello, ${arg.args[0]}!`;
	};
'

let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => {
			const text: string = await greet.build("World");
			return text;
		};
	'
}
let output = tg interface ($path | path join greet.tg.json) | complete
success $output
let expected = {
	kind: command,
	args: { kind: tuple, elements: [{ type: { kind: string } }] },
	output: { kind: string },
}
assert equal ($output.stdout | from json | get exports.greet.type) $expected
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# A wrong use fails checking in TypeScript and in Python.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => greet.build(42);
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:2:') $output.stderr
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
	tangram.py: '
		# /// script
		# [tool.tangram.dependencies.greet]
		# specifier = "./greet.tg.json"
		# ///
		from greet import greet
		async def default() -> int:
		    return await greet.build("World")
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.py:7:') $output.stderr

# An interface beside the config is used instead of the runtime's.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	greet.tg.tgi: '{ "exports": { "hello": { "type": { "kind": "command", "args": { "kind": "tuple", "elements": [] }, "output": { "kind": "number" } } } } }'
	runtime: { tangram.ts: $runtime }
}
let output = tg interface ($path | path join greet.tg.json) | complete
success $output
assert equal ($output.stdout | from json | get exports | columns) [hello]

# An interface beside the config may import a module relative to the config, which resolves like the config's dependencies.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime", "dependencies": { "greeting": "./greeting.tg.ts" } }'
	greet.tg.tgi: '{ "imports": { "greeting": { "kind": "typescript", "referent": { "node": "./greeting.tg.ts" } } }, "exports": { "greet": { "type": { "kind": "command", "args": { "kind": "tuple", "elements": [] }, "output": { "kind": "reference", "import": "greeting", "name": "Greeting" } } } } }'
	greeting.tg.ts: 'export interface Greeting { text: string; }'
	runtime: { tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => { const text: number = (await greet.build()).text; return text; };
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'") $output.stderr
