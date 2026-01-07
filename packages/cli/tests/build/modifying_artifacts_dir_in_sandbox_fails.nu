use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async () => {
			const bb = tg.build(busybox);
			const file = await tg.build`echo "Hello, World!" > ${tg.output}`.env(bb);
			await tg.run`echo "Goodbye, Reproducibility!" > ${file}`.env(bb);
			return file;
		}
	'
}

let output = tg build $path | complete
failure $output
