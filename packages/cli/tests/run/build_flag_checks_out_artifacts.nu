use ../../test.nu *

# `tg run --build .` checks out artifacts referenced in the command before executing.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const script = tg.file(
				"#!/bin/sh\necho \"Hello from checked-out artifact\"",
				{ executable: true },
			);
			const dir = tg.directory({
				bin: tg.directory({ run: script }),
			});
			return tg.command`${dir}/bin/run`;
		}
	'
}

# The command's args template references a directory artifact that must be checked out.
let output = tg run --build $path | complete
success $output
snapshot $output.stdout '
	Hello from checked-out artifact

'
