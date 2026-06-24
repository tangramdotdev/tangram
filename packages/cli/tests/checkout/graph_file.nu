use ../../test.nu *

# Checking out a file defined through a graph node materializes the file on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [{
					kind: "file",
					contents: "Hello, World!",
					executable: false,
				}],
			});
			return tg.file({ graph, index: 0, kind: "file" });
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
