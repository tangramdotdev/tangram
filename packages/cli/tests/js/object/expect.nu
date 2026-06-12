use ../../../test.nu *

# tg.Object.expect returns the value unchanged when it is an object.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			return tg.Object.expect(file) instanceof tg.File;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
