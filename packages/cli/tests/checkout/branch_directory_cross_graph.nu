use ../lib/test.nu *

# Flattening a branch keeps the graph context of both nested branches and leaf entries.
let local = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			const b = tg.Graph.withObject({ nodes: [
				{ kind: "directory", children: [{ directory: 1, count: 1, last: "target" }] },
				{ kind: "directory", entries: { target: 2 } },
				{ kind: "file", contents: await tg.blob("graph B"), dependencies: {}, executable: false, module: null },
			] });
			const a = tg.Graph.withObject({ nodes: [
				{ kind: "directory", children: [{ directory: { graph: b, index: 0, kind: "directory" }, count: 1, last: "target" }] },
				{ kind: "directory", entries: { target: 2 } },
				{ kind: "file", contents: await tg.blob("graph A"), dependencies: {}, executable: false, module: null },
			] });
			return a.get(0);
		}
	'
}
let id = tg build $path | str trim
let external = mktemp --directory | path join checkout
tg checkout $id --path $external
assert equal (open --raw ($external | path join target)) "graph B"
let internal = tg checkout $id | str trim
assert equal (open --raw ($internal | path join target)) "graph B"
