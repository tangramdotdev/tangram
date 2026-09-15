use ../../test.nu *

# A small array of stored files should fit in a child process argument.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export function child(files: tg.File[]) {
			return files.length;
		}
		export default async function () {
			const files = await Promise.all(Array.from({ length: 19 }, async (_, i) => {
				const file = await tg.file(`${i}`);
				await file.store();
				return file;
			}));
			return tg.build(child, files);
		}
	'
}

let output = tg build $path | complete
success $output
assert equal ($output.stdout | str trim) '19'
