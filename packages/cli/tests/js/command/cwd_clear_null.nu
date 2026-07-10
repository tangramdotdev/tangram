use ../../../test.nu *

# A null cwd override clears the inherited working directory, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				cwd: "/work",
			});
			let viaObject = await tg.command(base, { cwd: null });
			let viaFluent = await tg.command(base).cwd(null);
			let objectCwd = await viaObject.cwd;
			let fluentCwd = await viaFluent.cwd;
			return objectCwd === null && fluentCwd === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
