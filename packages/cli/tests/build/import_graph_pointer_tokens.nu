use ../lib/test.nu *

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	tracing: {
		filter: 'tangram=info,tangram_index::authorize::engine=debug'
		stderr_format: 'json'
	}
}
let alice = tg login --verbose --name alice | from json

# Importing the module's own directory creates a cycle, so checkin stores it as a graph.
let path = artifact {
	tangram.ts: '
		import source from "." with { type: "directory" };
		export default async function () {
			const pointer = await source.object();
			tg.assert("index" in pointer && pointer.graph);
			const graph = pointer.graph;
			const tokens = source.state.tokens.local ?? [];
			tg.assert(tokens.some((token) => {
				const body = JSON.parse(tg.encoding.utf8.decode(tg.encoding.base64.decode(token.split(".")[1]!)));
				return body.resource === graph.id && body.permissions.includes("object_subtree");
			}), "the directory should already have an exact graph subtree token");
			return { graph: graph.id, entries: Object.keys(await source.entries) };
		}
	'
}
let output = tg --token $alice.token build $path | from json
assert equal $output.entries ['tangram.ts']
server stop $server

# Reading the graph should use the available token without searching the authorization index.
let searches = open --raw $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { from json }
	| where $it.fields.message? == 'authorize batch'
	| where $it.fields.resource? == $output.graph
	| get fields.reads
assert equal $searches [] 'reading the imported directory searched the authorization graph despite having an exact token.'
