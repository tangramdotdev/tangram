use ../lib/test.nu *

# Reusing a child entry in a process output preserves the required authorization.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.build(createDirectory).then(tg.Directory.expect);
			let entries = await directory.entries;
			let child = entries["child"];
			return tg.directory({ reused: child });
		}

		export function createDirectory() {
			let directory = tg.directory({ leaf: tg.file("hi") });
			for (let i = 0; i < 18; i++) {
				directory = tg.directory({ child: directory });
			}
			return directory;
		}
	',
}
let out = tg build $path | complete
success $out "reusing a tokened directory's entry in a new output should authorize as subtree"
