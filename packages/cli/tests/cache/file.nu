use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!");
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
