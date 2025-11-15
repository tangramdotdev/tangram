use ../../test.nu *

let server = spawn

let path = artifact {
	'tangram.ts': '
		export default () => {
			let graph = tg.graph({
				nodes: [
					{
						kind: "file",
						contents: "Hello, World!",
					}
				]
			});
			return tg.file({ graph, node: 0 });
		}
	'
}

# Build the module.
let id = tg build $path | complete | get stdout | str trim

# Cache the artifact.
tg cache $id | complete | success

# Get the cached artifacts.
let artifacts_path = $server.directory | path join "artifacts"
snapshot --path $artifacts_path
