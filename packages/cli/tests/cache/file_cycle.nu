use ../../test.nu *

let server = spawn

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

# Cache.
let output = tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
