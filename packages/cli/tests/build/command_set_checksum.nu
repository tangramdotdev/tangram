use ../../test.nu *

# A build whose command sets an explicit sha256 checksum fails when the command is not eligible for a checksum.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(foo).checksum("sha256:bf5d7670a573508ae741a64acfd35f3e2a6bab3f9d02feda16495a2e622f2017");
		}
		export function foo() { return tg.file("Hello, World!"); }
	'
}

let output = tg build $path | complete
failure $output
