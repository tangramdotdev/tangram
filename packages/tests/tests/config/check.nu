use ../../util/test.nu *

# Checking a config module validates the config and its interface, and builds its check export when its interface has one, which passes or fails with its error.

let local = server spawn

let runtime = '
	export default async (arg: { export: string; args: Array<tg.Value>; package?: tg.Directory }) => {
		if (arg.export !== "check") {
			return null;
		}
		if (arg.args[0] === "package") {
			tg.assert(arg.package !== undefined, "expected the package");
		}
		if (arg.args[0] === "fail") {
			throw new Error("the runtime check failed");
		}
		return null;
	};
'
let interface = '// @tangram interface javascript
export declare const check: tg.Command<[], null>;'

let path = artifact {
	env.tg.json: '{ "runtime": "./runtime" }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
}
success (tg check ($path | path join env.tg.json) | complete)

let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "args": ["fail"] }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'the runtime check failed') $output.stderr

# The check of a package's root config receives the package.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime", "args": ["package"] }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
}
success (tg check $path | complete)

# A config whose interface has no check export checks without building one.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "args": ["fail"] }'
	runtime: { tangram.ts: $runtime }
}
success (tg check ($path | path join env.tg.json) | complete)

# An invalid interface fails checking.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime" }'
	env.tg.tgi: 'export declare const check: number[;'
	runtime: { tangram.ts: $runtime }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'invalid interface') $output.stderr

# An invalid config fails checking.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "imports": {} }'
	runtime: { config.tgi: $interface, tangram.ts: $runtime }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'unknown field') $output.stderr

# Checking a TypeScript package builds its check export, and a package without one checks without it.
let path = artifact {
	tangram.ts: '
		export default () => "Hello!";
		export let check = () => { throw new Error("the package check failed"); };
	'
}
let output = tg check $path | complete
failure $output
assert ($output.stderr | str contains 'the package check failed') $output.stderr
let path = artifact {
	tangram.ts: '
		export default () => "Hello!";
		export let check = () => null;
	'
}
success (tg check $path | complete)
let path = artifact {
	tangram.ts: 'export default () => "Hello!";'
}
success (tg check $path | complete)
