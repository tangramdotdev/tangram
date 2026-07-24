use ../../test.nu *

# Caching a directory containing a file that depends on its enclosing directory writes the directory into the artifacts cache.

let server = spawn --config { write: { cache_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default function () {
			return tg.directory({
				graph: tg.graph({
					nodes: [
						{ kind: "directory", entries: { "tangram.ts": 1 } },
						{ kind: "file", dependencies: { ".": 0 } },
					]
				}),
				index: 0,
				kind: "directory",
			})
		}
	'#
}
let id = tg build --no-cache-pointers $path
rm --recursive --force $server.cache_directory
mkdir $server.cache_directory

# Cache.
tg cache $id

snapshot --path $server.cache_directory
