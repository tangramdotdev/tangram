use ../../test.nu *

let tmp = mktemp -d

let server = spawn

let path = artifact {
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

let id = tg build $path
let checkout_path = $tmp | path join "checkout"
tg checkout $id $checkout_path
snapshot --path $checkout_path
