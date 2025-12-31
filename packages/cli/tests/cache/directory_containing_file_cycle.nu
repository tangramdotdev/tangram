use ../../test.nu *

let server = spawn

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

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
