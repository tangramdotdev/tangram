use ../../../test.nu *

# A file's dependencyObjects accessor returns its dependencies' objects as an array.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let dependency = await tg.file("dependency contents");
			let file = await tg
				.file("main")
				.dependency("./other.tg.ts", { item: dependency });
			let objects = await file.dependencyObjects;
			return [objects.length, await objects[0].text];
		}
	'
}

let output = tg build $path
snapshot $output '[1,"dependency contents"]'
