use ../../test.nu *

# Checking out a symlink defined through a graph node materializes the symlink on disk.

let tmp = mktemp --directory

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default function () {
			let graph = tg.graph({
				nodes: [{
					kind: "symlink",
					path: "/bin/sh",
				}],
			});
			return tg.symlink({ graph, index: 0, kind: "symlink" });
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout $id $path
snapshot --path $path
