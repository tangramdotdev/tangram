use ../../../lib/test.nu *

# tg.graph with nodes that have more than 1,024 children splits the nodes into leaves and a branch, and the graph's artifacts can still be read and checked out.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let graph = await create();
			let object = await graph.object();
			tg.assert("children" in object, "expected a branch");
			let directory = await graph.get(0);
			tg.Directory.assert(directory);
			let file = await directory.get("file_1099");
			tg.File.assert(file);
			return [object.children.length, (await graph.nodes()).length, await file.text];
		}

		export async function directory() {
			let graph = await create();
			return await graph.get(0);
		}

		async function create() {
			let nodes: Array<tg.Graph.Arg.Node> = [];
			let entries: { [name: string]: number } = {};
			for (let i = 0; i < 1100; i++) {
				entries[`file_${i}`] = i + 1;
			}
			nodes.push({ kind: "directory", entries });
			for (let i = 0; i < 1100; i++) {
				nodes.push({ kind: "file", contents: await tg.blob(`file ${i}`) });
			}
			return await tg.graph({ nodes });
		}
	'
}
let output = tg build $path | from json
assert equal ($output | get 0) 2 'the graph must have two leaves'
assert equal ($output | get 1) 1_101 'the graph must contain every node'
assert equal ($output | get 2) 'file 1099' 'the file must have the correct contents'

# Check out the directory.
let directory = tg build $'($path)#directory' | str trim
let checkout = mktemp -d | path join checkout
tg checkout $directory --path $checkout
assert equal (ls $checkout | length) 1_100 'the checkout must contain every entry'
assert equal (open ($checkout | path join file_1099)) 'file 1099' 'the checkout must have the correct contents'
