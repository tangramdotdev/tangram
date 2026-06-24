use ../../test.nu *

# The check command fails when a module calls a function that is not defined.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return foo(); }
	'
}

# Check.
let output = do { cd $path; tg check . } | complete
failure $output
