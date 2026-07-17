use ../../test.nu *

# A build whose child process writes a large amount to stdout hangs instead of completing.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(noisy);
		}
		export function noisy() {
			for (let i = 0; i < 200000; i++) {
				console.log("padding line " + i + " ------------------------------------------------------------");
			}
			return tg.file("done");
		}
	'
}

let output = tg build $path | complete
assert equal $output.exit_code 0 "the build hung awaiting the large-stdout child"
