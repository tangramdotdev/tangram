use ../../../test.nu *

# tg.sleep resolves to undefined.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => (await tg.sleep(0.05)) === undefined;'
}

let output = tg build $path
snapshot $output 'true'
