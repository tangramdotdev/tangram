use ../../util/test.nu *

# Publishing a config package checks its interface against the interface that its runtime generates, tags it with the tag in its config's metadata, and importers of the published package check against its interface.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let runtime = '
	export default async (arg: { export: string }) => {
		if (arg.export === "interface") {
			return tg.file({ contents: "export declare const greet: tg.Command<[], string>;\n", module: "interface_typescript" });
		}
		return "Hello!";
	};
'

# A package whose interface differs from its runtime's fails to publish, unless publishing writes the generated interface.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime", "metadata": { "tag": "greet/1.0.0" } }'
	runtime: { tangram.ts: $runtime }
}
let output = tg publish $path | complete
failure $output
assert ($output.stderr | str contains '--update-interfaces') $output.stderr
success (tg publish --update-interfaces $path | complete)
assert (open --raw ($path | path join tangram.tgi.ts) | str contains 'export declare const greet: tg.Command<[], string>;')
success (tg tag get greet/1.0.0 | complete)

let importer = artifact {
	tangram.ts: '
		import { greet } from "greet/^1";
		export default async () => { const text: number = await greet.build(); return text; };
	'
}
let output = tg check $importer | complete
failure $output
assert ($output.stderr | str contains 'tangram.ts:2:') $output.stderr

# A package whose interface matches its runtime's publishes.
success (tg publish --tag greet/1.0.1 $path | complete)
success (tg tag get greet/1.0.1 | complete)

# A package whose runtime does not generate an interface publishes with a warning, with a tag arg when its config has no tag in its metadata.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: 'export default async () => { throw new Error("unsupported"); };' }
}
let output = tg publish --tag hello/1.0.0 $path | complete
success $output
assert ($output.stderr | str contains 'warning') $output.stderr
success (tg tag get hello/1.0.0 | complete)

# Publishing checks the interfaces of every config in the packages that it publishes, including the configs that are not roots and the packages that are path dependencies.
let path = artifact {
	greet: {
		tangram.json: '{ "runtime": "../runtime", "metadata": { "tag": "greet-dependency/1.0.0" } }'
	}
	other.tg.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
	tangram.ts: '
		import * as greet from "./greet";
		import * as other from "./other.tg.json";
		export default () => null;
		export let metadata = { tag: "greet-importer/1.0.0" };
	'
}
let output = tg publish $path | complete
failure $output
assert ($output.stderr | str contains '--update-interfaces') $output.stderr
success (tg publish --update-interfaces $path | complete)
assert ($path | path join greet tangram.tgi.ts | path exists)
assert ($path | path join other.tgi.ts | path exists)

# Publishing writes the generated interface in its language, and removes the interface in another language.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime", "metadata": { "tag": "greet-json/1.0.0" } }'
	tangram.tgi.json: '{ "exports": {} }'
	runtime: { tangram.ts: $runtime }
}
success (tg publish --update-interfaces $path | complete)
assert ($path | path join tangram.tgi.ts | path exists)
assert not ($path | path join tangram.tgi.json | path exists)
