use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
success $output
