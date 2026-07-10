use ../../../test.nu *

# A null user override clears the inherited user via the object form.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				user: "nobody",
			});
			let cleared = await tg.command(base, { user: null });
			return (await cleared.user) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
