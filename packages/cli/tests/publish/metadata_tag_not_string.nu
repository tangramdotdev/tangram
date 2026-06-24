use ../../test.nu *

# Publishing a package whose metadata tag is not a string fails.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "x"; }

		export let metadata = {
			tag: 123,
		};
	'
}

let output = tg publish $path | complete
failure $output
snapshot ($output.stderr | redact $path) r#'
	error an error occurred
	-> failed to create publishing plan
	-> expected 'tag' to be a string
	   path = <path>/tangram.ts

'#
