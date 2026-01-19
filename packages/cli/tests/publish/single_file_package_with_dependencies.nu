use ../../test.nu *

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a single file (not a directory).
let path = artifact {
	root.tg.ts: '
		import * as directory from "directory" with { local: "./directory" };
		import * as file from "file" with { local: "./file.tg.ts" };
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
