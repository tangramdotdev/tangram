use ../../util/test.nu *

# Checking in a file with --lock=attr after --lock=file moves its unchanged lock from a sibling lockfile to the xattr.

let local = server spawn

let path = artifact {
	dependencies: {
		b.tg.ts: ''
		a.tg.ts: 'import "b";'
	}
	package: {
		main.tg.ts: '
			import "a";
		'
	}
}
tg tag b ($path | path join dependencies b.tg.ts)
tg tag a ($path | path join dependencies a.tg.ts)

let module_path = $path | path join package main.tg.ts
tg checkin --no-tokens --no-watch --lock=file $module_path | ignore
tg checkin --no-tokens --no-watch --lock=attr $module_path | ignore

# The lock should be in the xattr and not in the sibling lockfile.
assert (not ($path | path join package main.tg.lock | path exists)) 'the lockfile should not exist'
let xattrs = xattr_list $module_path | where { |name| $name == 'user.tangram.lock' }
assert (not ($xattrs | is-empty)) 'the xattr should exist'
