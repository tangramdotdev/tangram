use ../../test.nu *

# Caching a directory defined through a graph node writes the directory into the artifacts cache.

let server = spawn --config { write: { cache_pointers: false } }

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
rm --recursive --force ($server.directory | path join "artifacts")
mkdir ($server.directory | path join "artifacts")

# Cache.
tg cache $id

# Snapshot.
snapshot --path ($server.directory | path join "artifacts")
