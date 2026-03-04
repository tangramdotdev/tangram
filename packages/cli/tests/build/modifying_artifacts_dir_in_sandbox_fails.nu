use ../../test.nu *

let server = spawn --busybox

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async () => {
			const file = await tg.build`echo "Hello, World!" > ${tg.output}`.env(busybox);
			await tg.build`echo "Goodbye, Reproducibility!" > ${file}`.env(busybox);
		}
	'
}

let output = tg build $path | complete
failure $output
