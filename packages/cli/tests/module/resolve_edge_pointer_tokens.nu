use ../../test.nu *
use ../lib/module.nu *

# Resolving Edge::Pointer modules returns a token for their graph ID, so subsequent resolutions use exact-token authorization.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# The command makes the pointer to node A a child and provides the sole token Bob uses for the first resolution.
let path = artifact {
	tangram.ts: '
		export default async function () {
			const dependencyGraph = await tg.graph({
				nodes: [
					{ kind: "directory", entries: { "tangram.ts": 1 } },
					{ kind: "file", contents: `import "./c"; export default 2;`, dependencies: { "./c": 2 }, module: "ts" },
					{ kind: "file", contents: "export default 3;", module: "ts" },
				],
			});
			const graph = await tg.graph({
				nodes: [
					{
						kind: "file",
						contents: `import "./b"; export default 1;`,
						dependencies: { "./b": { graph: dependencyGraph, index: 0, kind: "directory" } },
						module: "ts",
					},
				],
			});
			const pointer: tg.Graph.Pointer = { graph, index: 0, kind: "file" };
			const module = new tg.Module({
				kind: "ts",
				referent: { node: pointer, options: {} },
			});
			const command = await tg.command({
				args: [tg.Command.Value.value(module)],
				executable: "tg",
				host: tg.host.current,
			});
			await command.store();
			module.referent.options ??= {};
			module.referent.options.tokens = command.state.tokens;
			return {
				dependencyGraph: dependencyGraph.id,
				graph: graph.id,
				module: tg.Module.toData(module),
			};
		}
	'
}

let case = tg --token $alice.token build $path | from json
tg --token $alice.token index
assert equal $case.module.referent.node $'graph=($case.graph)&index=0&kind=file'

let socket = $server.url | str replace 'http+unix://' '' | url decode
let watch = (
	tg --token $root_token checkpoint watch authorization.index
	| from json
	| get watch
)

# The first resolution must fall through to the index for the graph containing A.
let b_job = resolve-module-background $socket $bob.token $case.module './b'
let hit = tg --token $root_token checkpoint wait authorization.index $watch 0 | from json
assert equal $hit.params.resource $case.graph
tg --token $root_token checkpoint continue authorization.index $watch 0
let b = job recv --tag $b_job --timeout 10sec

assert equal (token-resource $b.module) $case.dependencyGraph

# Resolving B must complete without another index hit because it has an exact graph token.
let c_job = resolve-module-background $socket $bob.token $b.module './c'
let c = job recv --tag $c_job --timeout 10sec
assert equal (token-resource $c.module) $case.dependencyGraph
tg --token $root_token checkpoint unwatch authorization.index $watch
