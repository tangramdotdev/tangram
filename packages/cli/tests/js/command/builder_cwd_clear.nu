use ../../../test.nu *

# The builder's cwd method accepts null to clear the working directory.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg
				.command({ host: "builtin", executable: "echo", cwd: "/work" })
				.cwd(null);
			return (await command.cwd) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
