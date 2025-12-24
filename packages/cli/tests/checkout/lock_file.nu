use ../../test.nu *

# Test that --lock=file writes a sibling lockfile for a file with a tag dependency.

let tmp = mktemp -d

let server = spawn

let artifact = artifact {
	tangram.ts: '
		export default async () => {
			let bar = await tg.file("bar");
			return tg.file({
				contents: "foo",
				dependencies: {
					"bar": {
						item: bar,
						options: {
							id: bar.id,
							tag: "bar"
						}
					}
				}
			})
		}
	'
}
let id = tg build $artifact

let path = $tmp | path join "checkout"
tg checkout --lock=file --dependencies=false $id $path

# The sibling lockfile should exist.
let lockfile_path = $path | path parse | update extension "lock" | path join
assert ($lockfile_path | path exists)

# The xattr should not exist.
let xattrs = xattr_list $path | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
