use ../../test.nu *

# Test that --no-lock does not write any lock for a file with a tag dependency.

let server = spawn

let path = artifact {
	foo.tg.ts: ''
	bar.tg.ts: '
		import "foo"
	'
}

tg tag foo ($path | path join 'foo.tg.ts')
tg checkin --no-lock ($path | path join 'bar.tg.ts')

# The sibling lockfile should not exist.
let lockfile_path = $path | path join 'bar.tg.lock'
assert (not ($lockfile_path | path exists))

# The xattr should not exist.
let xattrs = xattr ($path | path join 'bar.tg.ts') | lines | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
