use ../../test.nu *

# Publishing with the dry run flag prints the plan and does not create the tag.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default () => "Hello, World!";

		export let metadata = {
			tag: "dry-pkg/1.0.0",
		};
	'
}

let output = tg publish --dry-run $path | complete
success $output

# The plan lists the package with its tag.
let plan = $output.stdout | from json
assert equal ($plan | get 0.tag) "dry-pkg/1.0.0" "the plan should list the package tag"

# The tag was not actually created.
let tag = tg tag get dry-pkg/1.0.0 | complete
failure $tag
