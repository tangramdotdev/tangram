use ../../../test.nu *

# A null executable override clears the inherited file executable flag, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.file({ contents: "x", executable: true });
			let viaObject = await tg.file(base, { executable: null });
			let viaFluent = await tg.file(base).executable(null);
			let objectExecutable = (await viaObject.object()).executable;
			let fluentExecutable = (await viaFluent.object()).executable;
			return objectExecutable === false && fluentExecutable === false;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
