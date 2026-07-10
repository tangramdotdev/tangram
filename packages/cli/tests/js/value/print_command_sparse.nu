use ../../../test.nu *

# tg.Value.print renders a command whose cwd, stdin, and user are absent without crashing and omits those fields.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({
				executable: "/bin/sh",
				host: "builtin",
			});
			let output = tg.Value.print(command);
			return (
				!output.includes(`"cwd":`) &&
				!output.includes(`"stdin":`) &&
				!output.includes(`"user":`)
			);
		}
	'
}

let output = tg build $path
snapshot $output 'true'
