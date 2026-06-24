use ../../../test.nu *

# tg.resolve passes undefined through unchanged.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let resolved = await tg.resolve(undefined);
			return resolved === undefined ? "undefined" : "other";
		}
	'
}

let output = tg build $path
snapshot $output '"undefined"'
