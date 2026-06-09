use ../../test.nu *

# Publishing a package whose metadata omits the tag field fails.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "x";

		export let metadata = {
			description: "no tag here",
		};
	'
}

let output = tg publish $path | complete
failure $output
assert ($output.stderr | str contains "metadata is missing the 'tag' field") "the error should mention the missing tag field"
