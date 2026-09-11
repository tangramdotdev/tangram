use ../../../test.nu *

# Loading an object attaches the returned exact tokens to each child handle.

let server = server spawn
let path = artifact {
	tangram.ts: '
		export default async function () {
			const directoryId = "dir_010000000000000000000000000000000000000000000000000000" as tg.Directory.Id;
			const fileId = "fil_010000000000000000000000000000000000000000000000000000" as tg.File.Id;
			const tokens = { local: ["child"] };
			const getObject = tg.client.getObject;
			try {
				tg.client.getObject = async () => ({
					children: { [fileId]: { tokens } },
					data: { kind: "directory", value: { entries: { a: fileId, b: fileId } } },
					tokens: { local: ["parent"] },
				});
				const directory = tg.Directory.withId(directoryId);
				const entries = await directory.entries;
				const children = await directory.state.children;
				tokens.local[0] = "mutated";
				return {
					children: children.map((child) => child.state.tokens),
					entries: Object.values(entries).map((child) => child.state.tokens),
					parent: directory.state.tokens,
				};
			} finally {
				tg.client.getObject = getObject;
			}
		}
	'
}
let output = tg build $path | from json
assert equal $output {
	children: [{ local: [child parent] } { local: [child parent] }]
	entries: [{ local: [child parent] } { local: [child parent] }]
	parent: { local: [parent] }
}
