use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [{
					kind: "symlink",
					path: "/bin/sh",
				}],
			});
			return tg.symlink({ graph, node: 0 });
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
run tg checkout $id $path
snapshot --path $path
