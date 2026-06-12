use ../../../test.nu *

# tg.Object.withId returns an object of the kind named by the id, preserving the id.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let file = await tg.file("hi");
			let object = tg.Object.withId(file.id);
			return object instanceof tg.File && object.id === file.id;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
