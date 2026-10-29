use ../../util/test.nu *

# Checking a config module validates the config and runs its runtime's check export, which passes or fails with its error.

let local = server spawn

let runtime = '
	export default async () => null;
	const getInterface = () => ({ exports: {} });
	export { getInterface as interface };
	export let check = async (arg: { args: Array<tg.Value>; package?: tg.Directory }) => {
		if (arg.args[0] === "package") {
			tg.assert(arg.package !== undefined, "expected the package");
		}
		if (arg.args[0] === "fail") {
			throw new Error("the runtime check failed");
		}
	};
'

let path = artifact {
	env.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
}
success (tg check ($path | path join env.tg.json) | complete)

let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "args": ["fail"] }'
	runtime: { tangram.ts: $runtime }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'the runtime check failed') $output.stderr

# The check of a package's root config receives the package.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime", "args": ["package"] }'
	runtime: { tangram.ts: $runtime }
}
success (tg check $path | complete)

# An invalid config fails checking.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "imports": {} }'
	runtime: { tangram.ts: $runtime }
}
let output = tg check ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'unknown field') $output.stderr
