use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => foo();
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
failure $output
