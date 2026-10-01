use ../lib/test.nu *

# Duplicate directory entries retain build authorization when authorization searches are disabled.

let local = server spawn --name local --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file("hello");
			const directory = await tg.directory({ a: file, b: file });
			await tg.build`true ${directory}`;
			return true;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
