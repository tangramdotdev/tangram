use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			return await tg.run(foo).checksum("sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017");
		};
		export let foo = () => tg.file("Hello, World!");
	'
}

let output = tg build $path | complete
failure $output
