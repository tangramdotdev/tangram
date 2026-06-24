use ../../../test.nu *

# A file created without dependencies has an empty dependencies map.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return await (await tg.file("hello")).dependencies; }'
}

let output = tg build $path
snapshot $output '{}'
