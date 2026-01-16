use ../../test.nu *

# Ensure that single file packages that import directory packages which have transitive
# dependencies using "../" imports work correctly when the transitive dependency has been
# pre-published. This tests the case where:
# - main.tg.ts imports ./subdir (a directory package)
# - subdir/tangram.ts imports ../sibling.tg.ts (using parent directory reference)
# - sibling.tg.ts has been pre-published (has an ID)
#
# The bug: When inherit() skips path resolution for items with IDs, the "../sibling.tg.ts"
# path is joined with the wrong parent directory (main.tg.ts's parent instead of
# subdir/tangram.ts's parent), resulting in an incorrect path.

let remote = spawn --cloud -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a directory structure that mimics the packages repo:
# packages/
#   sibling.tg.ts      <- single file package
#   main.tg.ts         <- single file that imports ./subdir
#   subdir/            <- directory package
#     tangram.ts       <- imports ../sibling.tg.ts
let root_path = artifact {
	packages: {
		sibling.tg.ts: '
			export default () => "I am sibling!";

			export let metadata = {
				tag: "test-sibling/1.0.0",
			};
		'
		main.tg.ts: '
			import subdir from "test-subdir" with { local: "./subdir" };

			export default () => `Main using: ${subdir()}`;

			export let metadata = {
				tag: "test-main/1.0.0",
			};
		'
		subdir: {
			tangram.ts: '
				import sibling from "test-sibling" with { local: "../sibling.tg.ts" };

				export default () => `Subdir using: ${sibling()}`;

				export let metadata = {
					tag: "test-subdir/1.0.0",
				};
			'
		}
	}
}

# First, publish sibling so it has an ID. This simulates packages that have been
# previously published (like openssl in the packages repo).
cd $root_path
let sibling_publish = tg publish ./packages/sibling.tg.ts | complete
success $sibling_publish

# Verify sibling is tagged.
let sibling_tag = tg tag get test-sibling/1.0.0 | complete
success $sibling_tag

# Now publish the main package from the root directory. The subdir package imports
# sibling with "../sibling.tg.ts". Since sibling has an ID, inherit() previously
# skipped path resolution, leaving the path as "../sibling.tg.ts" which would be
# incorrectly resolved relative to main.tg.ts instead of subdir/tangram.ts.
let output = tg publish ./packages/main.tg.ts | complete
success $output

# Verify all packages are tagged on local.
let local_sibling_tag = tg tag get test-sibling/1.0.0 | from json | get item
let local_subdir_tag = tg tag get test-subdir/1.0.0 | from json | get item
let local_main_tag = tg tag get test-main/1.0.0 | from json | get item

# Verify all packages are tagged on remote.
let remote_sibling_tag = tg --url $remote.url tag get test-sibling/1.0.0 | from json | get item
let remote_subdir_tag = tg --url $remote.url tag get test-subdir/1.0.0 | from json | get item
let remote_main_tag = tg --url $remote.url tag get test-main/1.0.0 | from json | get item
assert equal $remote_sibling_tag $local_sibling_tag "Remote sibling tag does not match local tag."
assert equal $remote_subdir_tag $local_subdir_tag "Remote subdir tag does not match local tag."
assert equal $remote_main_tag $local_main_tag "Remote main tag does not match local tag."
