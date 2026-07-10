use ../../../test.nu *

# A null module override clears the inherited file module, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.file("export default 1;").module("js");
			let viaObject = await tg.file(base, { module: null });
			let viaFluent = await tg.file(base).module(null);
			return (await viaObject.module) === null && (await viaFluent.module) === null;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
