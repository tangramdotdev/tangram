use ../../test.nu *

# Publishing with a malformed tag override fails to parse the tag.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default function () { return "x"; }

		export let metadata = {
			tag: "ok-pkg/1.0.0",
		};
	'
}

let output = tg publish --tag "@@@bad" $path | complete
failure $output
snapshot ($output.stderr | redact $path) '
	error an error occurred
	-> failed to parse the tag
	-> invalid specifier component

'
