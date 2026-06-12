use ../../../test.nu *

# The builder's cwd method sets the working directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let command = await tg
				.command({ host: "builtin", executable: "echo" })
				.cwd("/c");
			return await command.cwd;
		};
	'
}

let output = tg build $path
snapshot $output '"/c"'
