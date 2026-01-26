use ../../test.nu *

# Reproduces a bug where the tree view shows incorrect paths for single-file
# packages with local imports.
#
# Structure (mimics real packages repo):
#   packages/libiconv.tg.ts      (single-file package)
#   packages/bash.tg.ts          (imports ./libiconv.tg.ts)
#   packages/bzip2/tangram.ts    (imports ../bash.tg.ts)
#
# Bug: The path for libiconv is stored as "libiconv.tg.ts" instead of
# "./libiconv.tg.ts". When inherited from bzip2's context via referent.inherit(),
# the path becomes "packages/bzip2/libiconv.tg.ts" instead of the correct
# "packages/libiconv.tg.ts".

let server = spawn

let path = artifact {
	packages: {
		libiconv.tg.ts: 'export let metadata = { name: "libiconv" };',
		bash.tg.ts: '
			import * as libiconv from "libiconv" with { local: "./libiconv.tg.ts" };
			export let metadata = { name: "bash" };
		',
		bzip2: {
			tangram.ts: '
				import * as bash from "bash" with { local: "../bash.tg.ts" };
				export let metadata = { name: "bzip2" };
			'
		}
	}
}

let bzip2_path = $path | path join packages/bzip2

# The value tree shows the stored path property.
let val_tree = tg tree $bzip2_path --kind=value --depth 10
print $val_tree

# The path should preserve the "./" prefix from the import statement.
# This test will FAIL until the bug is fixed.
assert ($val_tree | str contains 'path: "./libiconv.tg.ts"') "path should be ./libiconv.tg.ts, not libiconv.tg.ts"
