use ../../../test.nu *

# The builder's cwd method sets the working directory.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: tg.host.current, executable: "echo" })
				.cwd("/c");
			return await command.cwd;
		}
	'
}

let output = tg build $path
snapshot $output '"/c"'
