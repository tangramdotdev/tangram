use ../../../test.nu *

# A file's dependencies accessor returns each reference mapped to a referent for its object.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let dependency = await tg.file("dependency contents");
			let file = await tg
				.file("main")
				.dependency("./other.tg.ts", { item: dependency });
			let dependencies = await file.dependencies;
			return [
				Object.keys(dependencies),
				await dependencies["./other.tg.ts"].item.text,
			];
		};
	'
}

let output = tg build $path
snapshot $output '[["./other.tg.ts"],"dependency contents"]'
