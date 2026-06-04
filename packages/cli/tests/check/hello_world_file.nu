use ../../test.nu *

# The check command succeeds for a package whose default export returns a tg.file constructed from a string.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => tg.file("Hello, World!");
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
success $output
