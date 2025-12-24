use ../../test.nu *

# Test that --lock=attr writes an xattr for a file with a tag dependency.

let server = spawn

let path = artifact {
	foo.tg.ts: ''
	bar.tg.ts: '
		import "foo"
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
