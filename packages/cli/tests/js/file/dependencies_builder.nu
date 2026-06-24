use ../../../test.nu *

# The builder's dependencies method sets the dependency map directly.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let dependency = await tg.file("dependency contents");
			let file = await tg
				.file("main")
				.dependencies({ "./a.tg.ts": { item: dependency } });
			return Object.keys(await file.dependencies);
		}
	'
}

let output = tg build $path
snapshot $output '["./a.tg.ts"]'
