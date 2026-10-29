use ../../util/test.nu *

# Publishing a config package tags it with the tag in its config's metadata, and importers of the published package check against its runtime's interface.

let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let runtime = '
	export default async (arg: { export: string }) => {
		if (arg.export === "interface") {
			return {
				exports: {
					greet: { type: { kind: "command", args: { kind: "tuple", elements: [] }, output: { kind: "string" } } },
				},
			};
		}
		return "Hello!";
	};
'

let path = artifact {
	tangram.json: '{ "runtime": "./runtime", "metadata": { "tag": "greet/1.0.0" } }'
	runtime: { tangram.ts: $runtime }
}
success (tg publish $path | complete)
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

# A config package without a tag in its metadata needs a tag arg.
let path = artifact {
	tangram.json: '{ "runtime": "./runtime" }'
	runtime: { tangram.ts: $runtime }
}
success (tg publish --tag hello/1.0.0 $path | complete)
success (tg tag get hello/1.0.0 | complete)
