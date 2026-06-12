use ../../../test.nu *

# A process stdout reader's readAll method drains the whole stream as bytes, configured with the per-stream stdout setter.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`printf hi`.stdout("pipe").sandbox();
			let bytes = await process.stdout.readAll();
			await process.wait();
			return [bytes.length, tg.encoding.utf8.decode(bytes)];
		};
	'
}

let output = tg build $path
snapshot $output '[2,"hi"]'
