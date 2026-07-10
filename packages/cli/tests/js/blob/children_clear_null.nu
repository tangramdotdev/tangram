use ../../../test.nu *

# A null children override clears the inherited blob children.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let base = await tg.blob("contents");
			let cleared = await tg.blob(base, { children: null });
			return (await cleared.bytes).length === 0;
		}
	'
}

let output = tg build $path
snapshot $output 'true'
