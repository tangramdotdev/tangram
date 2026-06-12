use ../../../test.nu *

# A process stderr reader reads the standard error stream, configured independently with the per-stream stderr setter.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let process = await tg.spawn`echo oops 1>&2`.stderr("pipe").sandbox();
			let text = await process.stderr.text();
			await process.wait();
			return text;
		};
	'
}

let output = tg build $path
snapshot $output '"oops\n"'
