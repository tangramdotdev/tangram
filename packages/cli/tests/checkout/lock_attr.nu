use ../../test.nu *

# --lock=attr writes an xattr for a file with a tag dependency.

let tmp = mktemp --directory

let server = server spawn

let artifact = artifact {
	tangram.ts: '
		export default async function () {
			let bar = await tg.file("bar");
			return tg.file({
				contents: "foo",
				module: "ts",
				dependencies: {
					"bar": {
						node: bar,
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
tg checkout --lock=attr --dependencies=false $id --path $path

# The sibling lockfile should not exist.
let lockfile_path = $path | path parse | update extension "lock" | path join
assert (not ($lockfile_path | path exists))

# The xattr should exist.
let xattrs = xattr_list $path | where { |name| $name == 'user.tangram.lock' }
assert (not ($xattrs | is-empty))

# All file xattrs are present alongside the required lock xattr.
assert equal (xattr_read 'user.tangram.module' $path) 'ts'
assert (not (xattr_read 'user.tangram.token' $path | is-empty))
assert equal (xattr_read 'user.tangram.dependencies' $path | from json | length) 1
assert (not (xattr_read 'user.tangram.lock' $path | from json | get nodes | is-empty))

# Internal checkout and a subsequent external checkout retain the module xattr.
let internal = tg checkout $id
assert equal (xattr_read 'user.tangram.module' $internal) 'ts'
tg checkout --force --lock=attr --dependencies=false $id --path $path
assert equal (xattr_read 'user.tangram.module' $path) 'ts'
assert (not (xattr_read 'user.tangram.lock' $path | from json | get nodes | is-empty))
assert (not (xattr_read 'user.tangram.token' $path | is-empty))
