use ../../test.nu *

# Publishing a package whose metadata omits the tag field fails.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "x"; }

		export let metadata = {
			description: "no tag here",
		};
	'
}

let output = tg publish $path | complete
failure $output
snapshot ($output.stderr | redact $path) r#'
	error an error occurred
	-> failed to create publishing plan
	-> metadata is missing the 'tag' field
	   path = <path>/tangram.ts

'#
