use ../../test.nu *

# Test that `tg run -b .` caches artifacts referenced in the command before executing.

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
			return tg.command({
				args: ["-c", tg`${dir}/bin/run`],
				executable: { path: "/bin/sh" },
				host: tg.process.env.TANGRAM_HOST,
			});
		};
	'
}

# The command's args template references a directory artifact that must be cached.
let output = tg run -b $path | complete
success $output
snapshot $output.stdout '
	Hello from cached artifact

'
