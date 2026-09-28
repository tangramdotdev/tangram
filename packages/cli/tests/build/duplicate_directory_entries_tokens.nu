use ../lib/test.nu *

let server = server spawn --config {
	authorization: {
		final: false
		initial: false
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
