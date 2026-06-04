use ../../test.nu *

# Caching two files that depend on each other, forming a cycle, writes the files into the artifacts cache.

let server = spawn --config { write: { cache_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.file({
				graph: tg.graph({
					nodes: [
						{ kind: "file", dependencies: { "./bar.tg.ts": 1 } },
						{ kind: "file", dependencies: { "./foo.tg.ts": 0 } },
					]
				}),
				index: 0,
				kind: "file",
			})
		}
	'#
}
let id = tg build --no-cache-pointers $path
rm --recursive --force ($server.directory | path join "artifacts")
mkdir ($server.directory | path join "artifacts")

# Cache.
let output = tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
