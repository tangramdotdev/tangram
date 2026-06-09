use ../../test.nu *

# Publishing with a malformed tag override fails to parse the tag.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "x";

		export let metadata = {
			tag: "ok-pkg/1.0.0",
		};
	'
}

let output = tg publish --tag "@@@bad" $path | complete
failure $output
assert ($output.stderr | str contains "failed to parse the tag") "the error should mention the failed tag parse"
