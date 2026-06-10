use ../../test.nu *

# Checking out a directory defined through a graph node materializes the directory on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [
					{
						kind: "directory",
						entries: { "hello.txt": tg.file("Hello, World!") },
					},
				],
			});
			return tg.directory({ graph, index: 0, kind: "directory" });
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
