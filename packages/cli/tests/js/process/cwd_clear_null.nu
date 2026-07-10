use ../../../test.nu *

# A null cwd override clears the inherited cwd, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				cwd: "/work",
			});
			let viaObject = await tg.spawn(base, { cwd: null }).sandbox();
			let viaFluent = await tg.spawn(base).cwd(null).sandbox();
			let objectCwd = await (await viaObject.command).cwd;
			let fluentCwd = await (await viaFluent.command).cwd;
			return objectCwd === null && fluentCwd === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
