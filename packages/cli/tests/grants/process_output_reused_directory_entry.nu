use ../../test.nu *

# Reusing a child from a tokened directory's `.entries` (which carries the parent's
# subtree token) in a new directory output must authorize as subtree. The object-batch
# advisory check rejects the ancestor token, downgrading the output to a node-only grant
# that then fails finish once its subtree exceeds the search budget (depth here).

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let raw = await tg.build(makeRaw).then(tg.Directory.expect);
			let entries = await raw.entries;
			let child = entries["child"];
			return tg.directory({ reused: child });
		}
		export function makeRaw() {
			let dir = tg.directory({ leaf: tg.file("hi") });
			for (let i = 0; i < 18; i++) {
				dir = tg.directory({ child: dir });
			}
			return dir;
		}
	',
}
let out = tg build $path | complete
success $out "reusing a tokened directory's entry in a new output should authorize as subtree"
