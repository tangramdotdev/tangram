use ../../util/test.nu *

# A config module's interface is the interface beside it, or else its runtime's interface for configs, or else an interface with no exports, and importers in TypeScript and Python check against it.

let local = server spawn

let runtime = '
	export default async (arg: { export: string; args: Array<tg.Value> }) => {
		tg.assert(arg.export === "greet");
		return `Hello, ${arg.args[0]}!`;
	};
'
let interface = {
	exports: {
		greet: {
			type: {
				kind: command,
				args: { kind: tuple, elements: [{ type: { kind: string } }] },
				output: { kind: string },
			},
		},
	},
} | to json

# The interface beside a config is its interface.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	greet.tg.tgi: $interface
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
assert equal ($output.stdout | from json | get exports.greet.type) ($interface | from json | get exports.greet.type)
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# A wrong use fails checking in TypeScript and in Python.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	greet.tg.tgi: $interface
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
	greet.tg.tgi: $interface
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

# A config without an interface beside it has its runtime's interface for configs.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => {
			const text: string = await greet.build("World");
			return text;
		};
	'
}
success (tg check $path | complete)
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# The interface beside a config replaces its runtime's interface for configs.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	greet.tg.tgi: '{ "exports": {} }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => greet.build("World");
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:1:') $output.stderr

# A config without an interface has no exports, but its exports still build.
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
	tangram.ts: '
		import { greet } from "./greet.tg.json";
		export default async () => greet.build("World");
	'
}
let output = tg interface ($path | path join greet.tg.json) | complete
success $output
assert equal ($output.stdout | from json | get exports) {}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:1:') $output.stderr
let output = tg build $'($path | path join greet.tg.json)#greet' --arg-string World | complete
success $output
assert equal ($output.stdout | from json) 'Hello, World!'

# An interface may import a module relative to itself, in the config's directory or in the runtime package.
let greeting = {
	imports: { greeting: { kind: typescript, referent: { node: './greeting.tg.ts' } } },
	exports: {
		greet: {
			type: {
				kind: command,
				args: { kind: tuple, elements: [] },
				output: { kind: reference, import: greeting, name: Greeting },
			},
		},
	},
} | to json
let source = '
	import { greet } from "./greet.tg.json";
	export default async () => { const text: number = (await greet.build()).text; return text; };
'
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	greet.tg.tgi: $greeting
	greeting.tg.ts: 'export interface Greeting { text: string; }'
	runtime: { tangram.ts: 'export default async () => ({ text: "Hello!" });' }
	tangram.ts: $source
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'") $output.stderr
let path = artifact {
	greet.tg.json: '{ "runtime": "./runtime" }'
	runtime: {
		config.tgi: $greeting
		greeting.tg.ts: 'export interface Greeting { text: string; }'
		tangram.ts: 'export default async () => ({ text: "Hello!" });'
	}
	tangram.ts: $source
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains "Type 'string' is not assignable to type 'number'") $output.stderr
