use ../../test.nu *

# Publishing a package whose metadata tag is not a string fails.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "x";

		export let metadata = {
			tag: 123,
		};
	'
}

let output = tg publish $path | complete
failure $output
assert ($output.stderr | str contains "expected 'tag' to be a string") "the error should mention that the tag must be a string"
