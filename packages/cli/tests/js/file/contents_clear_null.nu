use ../../../test.nu *

# A null contents override clears the inherited file contents, and the object and fluent forms are equivalent.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.file("original");
			let viaObject = await tg.file(base, { contents: null });
			let viaFluent = await tg.file(base).contents(null);
			let objectText = await viaObject.text;
			let fluentText = await viaFluent.text;
			return objectText === "" && fluentText === "";
		}
	'
}

let output = tg build $path
snapshot $output 'true'
