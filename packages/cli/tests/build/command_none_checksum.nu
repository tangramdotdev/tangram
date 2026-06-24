use ../../test.nu *

# A build whose command requests a checksum of "none" fails.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(foo).checksum("none");
		}
		export function foo() { return tg.file("Hello, World!"); }
	'
}

let output = tg build $path | complete
failure $output
