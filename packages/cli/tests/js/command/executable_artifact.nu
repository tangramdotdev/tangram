use ../../../test.nu *

# A command's executable accessor returns an artifact executable wrapped in an artifact field.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let file = await tg.file("#!/bin/sh\necho hi");
			let command = await tg.command({ host: "builtin", executable: file });
			let executable = await command.executable;
			return "artifact" in executable && executable.artifact instanceof tg.File;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
