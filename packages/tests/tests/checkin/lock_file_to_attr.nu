use ../../util/test.nu *

# Checking in a file with --lock=attr after --lock=file moves its lock from a sibling lockfile to the xattr, whether or not its tag dependency has a source import.

let local = server spawn

for import in ['import "b";' 'import "b" with { source: "./b.tg.ts" };'] {
	let path = artifact {
		dependencies: {
			b.tg.ts: ''
			a.tg.ts: $import
		}
		package: {
			main.tg.ts: '
				import "a";
			'
		}
	}
	tg tag --force b ($path | path join dependencies b.tg.ts)
	tg tag --force a ($path | path join dependencies a.tg.ts)

	let module_path = $path | path join package main.tg.ts
	tg checkin --no-tokens --no-watch --lock=file $module_path | ignore
	tg checkin --no-tokens --no-watch --lock=attr $module_path | ignore

	# The lock should be in the xattr and not in the sibling lockfile.
	assert (not ($path | path join package main.tg.lock | path exists)) $"the lockfile should not exist for ($import)"
	let xattrs = xattr_list $module_path | where { |name| $name == 'user.tangram.lock' }
	assert (not ($xattrs | is-empty)) $"the xattr should exist for ($import)"
}
