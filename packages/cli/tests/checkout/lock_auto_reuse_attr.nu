use ../../test.nu *

# Test that the default lock mode reuses an existing lockattr for a file dependency lock.

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
tg checkout --lock=attr --dependencies=false $id $path
tg checkout --force --dependencies=false $id $path

# The sibling lockfile should not exist.
let lockfile_path = $path | path parse | update extension "lock" | path join
assert (not ($lockfile_path | path exists))

# The xattr should exist.
let xattrs = xattr_list $path | where { |name| $name == 'user.tangram.lock' }
assert (not ($xattrs | is-empty))
