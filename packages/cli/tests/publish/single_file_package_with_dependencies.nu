use ../../test.nu *

# Publishing a single-file root module that imports both a directory dependency and a file dependency by source succeeds.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create a single file (not a directory).
let path = artifact {
	root.tg.ts: '
		import * as directory from "directory" with { source: "./directory" };
		import * as file from "file" with { source: "./file.tg.ts" };
		export let metadata = {
			tag: "root/1.0.0",
		};
	',
	file.tg.ts: '
		export let metadata = {
			tag: "file/1.0.0",
		};
	',
	directory: {
		tangram.ts: '
			export let metadata = {
				tag: "directory/1.0.0",
			};
		'
	}
};

let output = tg publish ($path | path join root.tg.ts) | complete
success $output
