use ../../../test.nu *

# A command's executable accessor returns a string executable as a path.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let command = await tg.command({ host: "builtin", executable: "echo" });
			return await command.executable;
		}
	'
}

let output = tg build $path
snapshot $output '{"path":"echo"}'
