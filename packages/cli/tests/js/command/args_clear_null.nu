use ../../../test.nu *

# A null args override clears the inherited argument list, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.command({
				host: "builtin",
				executable: "echo",
				args: ["a", "b"],
			});
			let viaObject = await tg.command(base, { args: null });
			let viaFluent = await tg.command(base).args(null);
			let objectArgs = await viaObject.args;
			let fluentArgs = await viaFluent.args;
			return objectArgs.length === 0 && fluentArgs.length === 0;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
