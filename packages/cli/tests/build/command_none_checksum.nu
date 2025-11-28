use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.run(foo).checksum("none");
		};
		export let foo = () => tg.file("Hello, World!");
	'
}

let output = tg build $path | complete
failure $output
