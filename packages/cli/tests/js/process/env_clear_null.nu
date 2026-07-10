use ../../../test.nu *

# A null env override clears the inherited command env.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				env: { FOO: "bar" },
			});
			let cleared = await tg.spawn(base, { env: null }).sandbox();
			let env = (await (await cleared.command).object()).env;
			return Object.keys(env).length === 0;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
