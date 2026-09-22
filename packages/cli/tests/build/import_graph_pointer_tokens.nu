use ../lib/test.nu *

# Reading a graph-backed object import should reuse its existing exact graph token.

def graph-searches [log: string, graph: string] {
	open --raw $log
		| lines
		| where ($it | str starts-with '{')
		| each { from json }
		| where $it.fields.message? == 'authorize batch'
		| where $it.fields.resource? == $graph
		| get fields.reads
}

let server = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
	tracing: {
		filter: 'tangram=info,tangram_index::authorize::engine=debug'
		stderr_format: 'json'
	}
}
let alice = tg login --verbose --name alice | from json
let module = '
	import source from "." with { type: "directory" };
	export default async function (forward: string) {
		const pointer = await source.object();
		tg.assert("index" in pointer && pointer.graph);
		const graph = pointer.graph;
		const hasExactToken = (tokens: tg.Tokens) =>
			(tokens.local?.authorization ?? []).some((token) => {
				const body = JSON.parse(tg.encoding.utf8.decode(tg.encoding.base64.decode(token.split(".")[1]!)));
				return body.resource === graph.id && body.permissions.includes("object_subtree");
			});
		const exactTokenAvailable = hasExactToken(source.state.tokens);
		const graphTokensBefore = graph.state.tokens.local?.authorization?.length ?? 0;
		if (forward === "true") graph.state.inheritTokens(source.state.tokens);
		const requests: Array<{ exactToken: boolean, tokenCount: number }> = [];
		const getObject = tg.client.getObject;
		try {
			tg.client.getObject = async (id, arg) => {
				if (id === graph.id) {
					requests.push({
						exactToken: hasExactToken(arg?.tokens ?? {}),
						tokenCount: arg?.tokens?.local?.authorization?.length ?? 0,
					});
				}
				return getObject.call(tg.client, id, arg);
			};
			const entries = Object.keys(await source.entries);
			return { entries, exactTokenAvailable, graph: graph.id, graphTokensBefore, requests };
		} finally {
			tg.client.getObject = getObject;
		}
	}
'

# Importing the leaf directory from itself creates a real cyclic graph during checkin.
# The wrappers increase the distance from the process command to that graph.
let measurements = [0 16 64] | each { |depth|
	mut files = { leaf: { tangram.ts: ($module + $'// Dependency depth: ($depth).') } }
	mut reference = './leaf'
	for index in 0..<$depth {
		let name = $'step($index).tg.ts'
		$files = $files | insert $name $'import value from "($reference)"; export default value;'
		$reference = $'./($name)'
	}
	$files = $files | insert 'tangram.ts' $'import value from "($reference)"; export default value;'
	let path = artifact $files
	let normal = tg --token $alice.token build $path -- false | from json
	let searches = graph-searches $server.log $normal.graph
	assert $normal.exactTokenAvailable 'the object import should already hold an exact graph subtree proof.'
	assert equal $normal.entries ['tangram.ts']
	assert equal ($normal.requests | length) 1 'the normal build must actually fetch the graph.'

	# A distinct process forwards the same proof, with fresh handles and the same graph.
	let forwarded = tg --token $alice.token build $path -- true | from json
	assert equal $forwarded.graph $normal.graph
	assert equal $forwarded.entries $normal.entries
	assert equal ($forwarded.requests | length) 1 'the control must actually fetch the graph.'
	assert $forwarded.requests.0.exactToken
	assert equal (graph-searches $server.log $normal.graph) $searches 'forwarding the proof should avoid every graph authorization search.'
	{
		depth: $depth
		graph_tokens_before: $normal.graphTokensBefore
		request_tokens: $normal.requests.0.tokenCount
		searches: ($searches | length)
		reads: ($searches | math sum)
		forwarded_searches: 0
	}
}
server stop $server
print ($measurements | to json)

assert ($measurements | all { $in.searches == 0 }) 'object imports dropped an available exact graph token and searched the authorization graph.'
