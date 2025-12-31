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
							"hello.txt": tg.file("Hello, World!")
						}
					}
				]
			});
			return tg.directory({
				graph,
				index: 0,
				kind: "directory"
			});
		}
	'
}
let id = tg checkin --no-cache-pointers $artifact
let id = tg build $id

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
