use ../../test.nu *

# --lock=attr writes an xattr for a file with a tag dependency.

let server = server spawn

let path = artifact {
	foo.tg.ts: ''
	bar.tg.ts: '
		import "foo?location=local"
	'
}

tg tag foo ($path | path join 'foo.tg.ts')
tg checkin --lock=attr ($path | path join 'bar.tg.ts')

# The sibling lockfile should not exist.
let lockfile_path = $path | path join 'bar.tg.lock'
assert (not ($lockfile_path | path exists))

# The xattr should exist.
let xattrs = xattr_list ($path | path join 'bar.tg.ts') | where { |name| $name == 'user.tangram.lock' }
assert (not ($xattrs | is-empty))

# The lock should not persist traversal credentials.
let lock_text = xattr_read 'user.tangram.lock' ($path | path join 'bar.tg.ts')
let lock = $lock_text | from json
let reference = $lock.nodes.0.dependencies | columns | first
assert equal $reference foo "the reference location should be stripped"
assert (not ($lock_text | str contains '"location"'))
assert (not ($lock_text | str contains '"tokens"'))
