use ../../test.nu *

let server = spawn

# Create the artifact.
let artifact = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [
					{
						kind: "directory",
						entries: {
							link: { node: 1 }
						}
					},
					{
						kind: "symlink",
						artifact: { node: 0 },
						path: "link"
					}
				]
			});
			return tg.symlink({
				graph,
				node: 0
			});
		}
	'
}
let id = run tg build $artifact

# Cache.
run tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
