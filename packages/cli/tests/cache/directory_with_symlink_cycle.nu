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
							link: { index: 1, kind: "symlink" }
						}
					},
					{
						kind: "symlink",
						artifact: { index: 0, kind: "directory" },
						path: "link"
					}
				]
			});
			return tg.symlink({
				graph,
				index: 0,
				kind: "directory"
			});
		}
	'
}
let id = tg checkin --no-cache-references $artifact
let id = tg build $id

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
