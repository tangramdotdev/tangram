use ../../lib/test.nu *

# Building through a graph pointer preserves the tokens needed to authorize its imports.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json

# Importing the module's own directory creates a cycle, so checkin stores it as a graph.
let path = artifact {
	tangram.ts: '
		import source from "." with { type: "directory" };
		export default async function () {
			const pointer = await source.object();
			tg.assert("index" in pointer && pointer.graph);
			return { entries: Object.keys(await source.entries) };
		}
	'
}
let output = tg --token $alice.token build $path | from json
assert equal $output.entries ['tangram.ts']
