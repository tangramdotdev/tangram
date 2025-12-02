use ../../test.nu *

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
