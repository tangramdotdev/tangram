use ../../test.nu *

# The check command succeeds for a minimal package whose default export returns a string.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "Hello, World!"; }
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
success $output
