use ../lib/test.nu *

# Both checkin modes normalize cached timestamps without modifying symlink targets.
for destructive in [false, true] {
	let local = server spawn --config { vfs: false }
	let target = artifact 'external target'
	let target_modified = ls $target | first | get modified
	let path = artifact {
		file: 'hello'
		link: (symlink $target)
		missing: (symlink 'missing-target')
	}
	let flags = if $destructive { ['--destructive'] } else { [] }
	let id = tg checkin --no-ignore ...$flags $path | str trim
	let checkout = tg checkout $id | str trim
	for path in [$checkout, ($checkout | path join file), ($checkout | path join link), ($checkout | path join missing)] {
		assert equal (ls -D $path | first | get modified) 1970-01-01T00:00:00Z
	}
	assert equal (ls $target | first | get modified) $target_modified
	server stop $local
}
