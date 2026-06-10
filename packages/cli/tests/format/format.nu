use ../../test.nu *

# Formatting a package reformats its TypeScript modules while respecting the tangramignore file, producing a tree that matches the snapshot.

let server = spawn

let temp_dir = mktemp --directory

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

# Format.
tg format $path

# Snapshot.
snapshot --path $path
