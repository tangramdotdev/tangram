use ../../test.nu *

let server = spawn --config { write: { cache_pointers: false } }

let path = artifact {
	tangram.ts: r#'
		export default () => {
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

# Cache.
tg cache $id

snapshot --path ($server.directory | path join "artifacts")
