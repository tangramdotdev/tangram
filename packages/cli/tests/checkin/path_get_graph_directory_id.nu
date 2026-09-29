use ../lib/test.nu *

# Resolving a get path through a directory object preserves its graph context.
let local = server spawn
let dependency_path = artifact {
	tangram.ts: '
		export default async function () {
			const graph = await tg.graph({ nodes: [
				{ kind: "directory", entries: { "module.tg.ts": 1 } },
				{ kind: "file", contents: `export default function () { return "graph target"; }`, module: "ts" },
			] });
			return graph.get(0);
		}
	'
}
let dependency = tg build --no-tokens $dependency_path | str trim
let reference = $'($dependency)?get=module.tg.ts'
let dependencies = [$reference] | to json
let path = artifact (file --xattrs { "user.tangram.dependencies": $dependencies } input)
let id = tg checkin --no-solve $path
let object = tg get --blobs --depth=inf --no-tokens --pretty $id
assert ($object | str contains "graph target") "the dependency should refer to the file in the loaded graph"
