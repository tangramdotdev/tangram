use ../../test.nu *

# A build produces a deep directory, and the output reuses one of its deep entries nested inside
# another directory. The reused entry is external and carries only an ancestor token whose resource
# is the original directory, not the entry. Building this output must authorize it as a subtree.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let directory = await tg.build(createDirectory).then(tg.Directory.expect);
			let entries = await directory.entries;
			let child = entries["child"];
			return tg.directory({ wrap: tg.directory({ reused: child }) });
		}

		export function createDirectory() {
			let directory = tg.directory({ leaf: tg.file("hi") });
			for (let i = 0; i < 20; i++) {
				directory = tg.directory({ child: directory });
			}
			return directory;
		}
	',
}
let out = tg build $path | complete
success $out "reusing a deep directory entry nested in a new output should authorize as subtree"
