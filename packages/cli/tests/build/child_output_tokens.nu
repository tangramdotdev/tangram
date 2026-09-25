use ../lib/test.nu *

# A child's output is readable through the tokens its wait returns when authorization searches are disabled.

let server = server spawn --config {
	authorization: {
		final: false
		initial: false
	}
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = tg.File.expect(await tg.build(child));
			return await file.text;
		}

		export function child() {
			return tg.file("hello");
		}
	'
}

let output = tg build $path
snapshot $output '"hello"'
