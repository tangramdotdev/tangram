use ../../test.nu *

let server = spawn

let temp_dir = mktemp -d

# Create a package.
let path = artifact {
	.tangramignore: 'foo'
	foo.ts: 'export default   "not formatted"'
	bar.tg.ts: 'export default   "formatted"'
	foo: (directory {
		tangram.ts: 'export default   "not formatted"'
	})
	bar: (directory {
		tangram.ts: 'export default   "formatted"'
	})
	tangram.ts: 'export default   "formatted"'
}

# Format the package.
let output = tg format $path | complete
success $output

# Snapshot the package.
snapshot --path $path
