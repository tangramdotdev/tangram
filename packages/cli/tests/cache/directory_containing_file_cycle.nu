use ../../test.nu *

# Caching a directory containing a file that participates in a dependency cycle writes the directory into the artifacts cache.

let server = spawn --config { write: { cache_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default () => {
			let graph = tg.graph({
				nodes: [
					{ kind: "file", dependencies: { "./bar.tg.ts": 1 } },
					{ kind: "file", dependencies: { "./foo.tg.ts": 0 } },
				]
			});
			let foo = tg.file({
				graph,
				index: 0,
				kind: "file",
			});
			return tg.directory({
				foo,
			});
		}
	'#
}
let id = tg build --no-cache-pointers $path
rm --recursive --force ($server.directory | path join "artifacts")
mkdir ($server.directory | path join "artifacts")

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
