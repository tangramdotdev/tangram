use ../../util/test.nu *

# Building a config module runs its runtime's default export with the export name, the config's args followed by the caller's args, the package, and the dependencies.

let local = server spawn

let runtime = '
	export default async (arg: {
		export: string;
		args: Array<tg.Value>;
		package?: tg.Directory;
		dependencies: { [name: string]: { item: tg.Artifact } };
	}) => {
		const dependencies: { [name: string]: string } = {};
		for (const [name, dependency] of Object.entries(arg.dependencies)) {
			tg.File.assert(dependency.item);
			dependencies[name] = await dependency.item.text;
		}
		return {
			export: arg.export,
			args: arg.args,
			package: arg.package !== undefined,
			dependencies,
		};
	};
'

# A single-file config module in each format.
let configs = {
	env.tg.json: '{ "runtime": "./runtime", "args": ["config"], "dependencies": { "greeting": "./greeting.txt" } }'
	env.tg.toml: "runtime = \"./runtime\"\nargs = [\"config\"]\n\n[dependencies.greeting]\nspecifier = \"./greeting.txt\"\n"
	env.tg.yaml: "runtime: ./runtime\nargs:\n  - config\ndependencies:\n  greeting: ./greeting.txt\n"
}
for name in ($configs | columns) {
	let path = artifact {
		$name: ($configs | get $name)
		greeting.txt: 'Hello!'
		runtime: { tangram.ts: $runtime }
	}
	let output = tg build ($path | path join $name) | complete
	success $output
	let expected = {
		export: default,
		args: [config],
		package: false,
		dependencies: { greeting: 'Hello!' },
	}
	assert equal ($output.stdout | from json) $expected $name

	# Building an export passes its name, and the caller's args follow the config's args.
	let output = tg build $'($path | path join $name)#greet' --arg-string hey --arg-value '1' | complete
	success $output
	let output = $output.stdout | from json
	assert equal $output.export greet
	assert equal $output.args [config, hey, 1]
}

# A package whose root is a config module receives the package.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
}
let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json | get package) true

# A config with an unknown key is invalid.
let path = artifact {
	env.tg.json: '{ "runtime": "./runtime", "imports": {} }'
	runtime: { tangram.ts: $runtime }
}
let output = tg build ($path | path join env.tg.json) | complete
failure $output
assert ($output.stderr | str contains 'unknown field') $output.stderr
