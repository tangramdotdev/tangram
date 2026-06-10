use ../../test.nu *

# The check command succeeds for a package whose modules import each other in a cycle.

let server = spawn

let path = artifact {
	tangram.ts: '
		import "./foo.tg.ts"
	'
	foo.tg.ts: '
		import "./tangram.ts"
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
success $output
