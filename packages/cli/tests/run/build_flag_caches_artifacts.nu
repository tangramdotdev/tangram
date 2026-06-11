use ../../test.nu *

# `tg run --build .` caches artifacts referenced in the command before executing.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			const script = tg.file(
				"#!/bin/sh\necho \"Hello from cached artifact\"",
				{ executable: true },
			);
			const dir = tg.directory({
				bin: tg.directory({ run: script }),
			});
			return tg.command`${dir}/bin/run`;
		};
	'
}

# The command's args template references a directory artifact that must be cached.
let output = tg run --build $path | complete
success $output
snapshot $output.stdout '
	Hello from cached artifact

'
