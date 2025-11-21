use ../../test.nu *

let tmp = mktemp -d

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
			return tg.directory({ graph, node: 0 });
		}
	'
}
let id = run tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout $id $path
snapshot --path $path
