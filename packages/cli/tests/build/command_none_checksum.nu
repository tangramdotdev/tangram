use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.run`echo "Hello, World!" > $OUTPUT`.checksum("none");
		};
	'
}

let output = tg build $path | complete
failure $output
