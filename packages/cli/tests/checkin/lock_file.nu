use ../../test.nu *

# Test that --lock=file writes a sibling lockfile for a file with a tag dependency.

let server = spawn

let path = artifact {
	foo.tg.ts: ''
	bar.tg.ts: '
		import "foo"
	'
}

tg tag foo ($path | path join 'foo.tg.ts')
tg checkin --lock=file ($path | path join 'bar.tg.ts')

# The sibling lockfile should exist.
let lockfile_path = $path | path join 'bar.tg.lock'
assert ($lockfile_path | path exists)

# The xattr should not exist.
let xattrs = xattr ($path | path join 'bar.tg.ts') | lines | where { |name| $name == 'user.tangram.lock' }
assert ($xattrs | is-empty)
