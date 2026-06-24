use ../../test.nu *

# Caching a reference to a directory that points into a cycle writes the directory into the artifacts cache.

let tmp = mktemp --directory

let server = spawn --config { write: { cache_pointers: false } }

let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [
					{ kind: "directory", entries: { "b": 1 } },
					{ kind: "directory", entries: { "c": 2 } },
					{ kind: "file", dependencies: { "a": 0 } },
				]
			})
			return tg.directory({ graph, index: 1, kind: "directory" });
		}
	'
}
let id = tg build --no-cache-pointers $artifact
rm --recursive --force ($server.directory | path join "artifacts")
mkdir ($server.directory | path join "artifacts")

let output = tg cache $id

snapshot --path ($server.directory | path join "artifacts")
