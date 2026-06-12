use ../../../test.nu *

# tg.Object.is is true for an object and false for non-object values.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			return [tg.Object.is(file), tg.Object.is({ a: 1 }), tg.Object.is("x")];
		};
	'
}

let output = tg build $path
snapshot $output '[true,false,false]'
