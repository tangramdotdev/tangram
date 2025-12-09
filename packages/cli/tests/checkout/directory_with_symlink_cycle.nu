use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [{
						kind: "directory",
						entries: {"link": 1}
					},
					{
						kind: "symlink",
						artifact: 0,
						path: "link"
					},
				],
			});
			return tg.directory({ graph, index: 0, kind: "directory" });
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $path
snapshot --path $path
