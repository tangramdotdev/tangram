use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			let graph = tg.graph({
				nodes: [
					{
						kind: "directory",
						entries: { link: { node: 1 } },
					},
					{
						kind: "symlink",
						artifact: { node: 0 },
						path: "link",
					}
				]
			});
			return tg.symlink({ graph, node: 0 });
		}
	'
}

# Build the module.
let id = tg build $path | complete | get stdout | str trim

# Cache the artifact.
let output = tg cache $id | complete

success $output

# Get the cached artifacts.
let artifacts_path = $server.directory | path join "artifacts"
snapshot --path $artifacts_path
