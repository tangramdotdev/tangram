use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
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
			return tg.directory({ graph, node: 0 });
		}
	'
}

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout --dependencies=true $id $checkout_path
snapshot --path $checkout_path
