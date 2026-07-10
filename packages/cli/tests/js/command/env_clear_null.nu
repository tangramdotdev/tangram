use ../../../test.nu *

# A null env override clears the inherited environment, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				env: { FOO: "bar" },
			});
			let viaObject = await tg.command(base, { env: null });
			let viaFluent = await tg.command(base).env(null);
			let objectEnv = (await viaObject.object()).env;
			let fluentEnv = (await viaFluent.object()).env;
			return Object.keys(objectEnv).length === 0 && Object.keys(fluentEnv).length === 0;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
